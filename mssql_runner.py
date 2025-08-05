"""
mcp_runner.py – routes Azure OpenAI function calls to two MCP servers:
  • pgserver   – Postgres read‑only SQL tools
  • wxserver   – demo calculator with an `add` tool
Run: uv run python mcp_runner.py
"""

import asyncio, json, os, sys
from typing import Any, Dict, List, cast

from dotenv import load_dotenv
from openai import AzureOpenAI
from openai.types.chat import ChatCompletionMessage, ChatCompletionMessageParam
from mcp.client.stdio import stdio_client, StdioServerParameters
from mcp import ClientSession

# ── env & Azure client ──────────────────────────────────────────────────────
load_dotenv()
client = AzureOpenAI(
    api_key        = os.environ["AOAI_KEY"],
    azure_endpoint = os.environ["AOAI_URL"],
    api_version    = os.getenv("OPENAI_API_VERSION") or "2023-05-15",
)
DEPLOYMENT = os.getenv("AOAI_DEPLOYMENT") or "pgdemo-gpt4"

# ── MCP servers (stdio) ─────────────────────────────────────────────────────
PG_SERVER = StdioServerParameters(
    command="uv",
    args=["run", "python", "-m", "pgserver"],
)
MSSQL_SERVER = StdioServerParameters(
    command="uv",
    args=["run", "python", "-m", "mssqlserver"],
)

TOOL_ROUTE: dict[str, StdioServerParameters] = {
    "query_sql_mssql":         MSSQL_SERVER,
}

# 🔍 A — sanity-check once at import time
assert TOOL_ROUTE["query_sql_mssql"] is MSSQL_SERVER, "Router mis-mapped!"

# ── helper to call any MCP tool ─────────────────────────────────────────────
async def run_tool(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    srv = TOOL_ROUTE[name]
    # print sql (if present) before the round-trip
    print("DEBUG env.MSSQL_DSN →", os.getenv("MSSQL_DSN")[:120], file=sys.stderr)
    async with stdio_client(srv) as (r, w):
        async with ClientSession(r, w) as sess:
            await sess.initialize()
            res = await sess.call_tool(name, args)

    # 🔍 B — log the raw structured content
    print("DEBUG result →", json.dumps(res.structuredContent)[:500], file=sys.stderr)

    return res.structuredContent  # type: ignore[attr-defined]


# ── function schemas for Azure (3 tools) ────────────────────────────────────
tool_schemas: List[Dict[str, Any]] = [
    {
        "name": "query_sql_mssql",                     # <-- pick a unique name
        "description": (
            "Run a read-only T-SQL statement (SELECT / WITH) against the "
            "Microsoft SQL Server warehouse and return rows as JSON. "
            "If the query omits TOP/OFFSET, the tool will automatically "
            "append 'OFFSET 0 ROWS FETCH NEXT <limit>' to cap the result."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "sql":   {"type": "string"},
                "limit": {"type": "integer", "default": 500},
            },
            "required": ["sql"]
        }
    }
]

# ── interactive chat loop ───────────────────────────────────────────────────
async def chat() -> None:
    messages: List[ChatCompletionMessageParam] = [
        {"role": "system", "content": SYSTEM_PROMPT}
    ]

    while True:
        user = input("❯ ")
        messages.append({"role": "user", "content": user})

        while True:
            resp = client.chat.completions.create(
                model        = DEPLOYMENT,
                temperature  = 0.2,
                messages     = messages,
                functions    = tool_schemas,
                function_call= "auto",
            )

            msg: ChatCompletionMessage = resp.choices[0].message
            call = getattr(msg, "function_call", None)

            if call:
                name = call.name
                args = json.loads(call.arguments)
                print(f"🔧 calling {name}{args}", file=sys.stderr)   # 🔍 C

                result = await run_tool(name, args)

                messages.append(cast(ChatCompletionMessageParam, msg))
                messages.append(cast(ChatCompletionMessageParam, {
                    "role": "function",
                    "name": name,
                    "content": json.dumps(result),
                }))
                continue  # let GPT craft final answer

            print("Assistant →", (msg.content or "").strip())
            messages.append(cast(ChatCompletionMessageParam, msg))
            break

SYSTEM_PROMPT = """
You are a bilingual (中文 / English) financial-data assistant.

But we're currently only testing if you can connect to a microsoftSQL database.
"""



# ── main ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        asyncio.run(chat())
    except KeyboardInterrupt:
        sys.exit(0)
