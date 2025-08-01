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
WX_SERVER = StdioServerParameters(
    command="uv",
    args=["run", "python", "-m", "wxserver"],
)

TOOL_ROUTE: dict[str, StdioServerParameters] = {
    "list_tables": PG_SERVER,
    "query_sql":   PG_SERVER,
    "resolve_stock_id": PG_SERVER,
    "describe_table": PG_SERVER,
    "resolve_stock_name": PG_SERVER,
    "add":         WX_SERVER,      # <── comes from wxserver
}

# ── helper to call any MCP tool ─────────────────────────────────────────────
async def run_tool(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    srv = TOOL_ROUTE[name]
    async with stdio_client(srv) as (r, w):
        async with ClientSession(r, w) as sess:
            await sess.initialize()
            res = await sess.call_tool(name, args)
            return res.structuredContent  # type: ignore[attr-defined]

# ── function schemas for Azure (3 tools) ────────────────────────────────────
tool_schemas: List[Dict[str, Any]] = [
    {
        "name": "list_tables",
        "description": "Return an array of table names in the specified "
                       "PostgreSQL schema.",
        "parameters": {
            "type": "object",
            "properties": {"schema": {"type": "string", "default": "public"}},
        },
    },
    {
        "name": "query_sql",
        "description": "Run a read‑only SELECT / WITH / EXPLAIN query"
                       "and return rows.",
        "parameters": {
            "type": "object",
            "properties": {
                "sql":   {"type": "string"},
                "limit": {"type": "integer", "default": 500},
            },
            "required": ["sql"],
        },
    },
    {"name": "resolve_stock_id",
        "description": "Translate a Taiwanese stock name or ticker to its numeric ID.",
        "parameters": {
            "type":"object",
            "properties":{"name":{"type":"string"}},
            "required":["name"]
        },
    },
    {"name": "describe_table",
        "description": "Get column info for a table.",
        "parameters": {
            "type": "object",
            "properties": {
                "table":  {"type": "string"},
                "schema": {"type": "string", "default": "public"}
            },
            "required": ["table"]
        },
    },
    {
    "name": "resolve_stock_name",
    "description": "Return the common company name for a given stock ID.",
    "parameters": {
        "type": "object",
        "properties": {"stock_id": {"type": "string"}},
        "required": ["stock_id"]
        },
    },
    {
        "name": "add",
        "description": "Add two numbers and return the sum.",
        "parameters": {
            "type": "object",
            "properties": {
                "a": {"type": "number"},
                "b": {"type": "number"},
            },
            "required": ["a", "b"],
        },
    },
]

# ── interactive chat loop ───────────────────────────────────────────────────
async def chat() -> None:
    messages: List[ChatCompletionMessageParam] = [
        {"role": "system",
         "content": SYSTEM_PROMPT
        }
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
                print(f"🔧 calling {name}{args}")

                result = await run_tool(name, args)

                messages.append(cast(ChatCompletionMessageParam, msg))
                messages.append(cast(ChatCompletionMessageParam, {
                    "role": "function",
                    "name": name,
                    "content": json.dumps(result),
                }))
                continue     # loop back: let GPT craft the natural answer

            print("Assistant →", (msg.content or "").strip())
            messages.append(cast(ChatCompletionMessageParam, msg))
            break           # waiting for next user prompt

SYSTEM_PROMPT = """
You are a bilingual (中文 / English) financial-data assistant.

Your mission:
• Help users explore Taiwanese stock fundamentals and prices.
• When a user mentions a company name or ticker (e.g., 台積電, TSMC, 鴻海),
If you need stock data, fetch it from the table "stock_quotes", 
for current price, please reference "AskPrice1",
for starting price, please reference "RefPrice"

**Ground rules**:
• NEVER execute INSERT/UPDATE/DELETE.
• NEVER guess a stock_id—always call `resolve_stock_id`.
• If a query would return more than 500 rows, aggregate or LIMIT 100.
• If data is unavailable, say so and suggest an alternative metric.
• ALWAYS call `describe_table` before query_sql for a table.
• Column names that contain capitals must be double-quoted, e.g. "AskPrice1", "MktDate" …”
• When outputting stock id, always run `resolve_stock_name` to get a representative name, and append it after the stock_id, i.e. "2330 台積電".
"""



# ── main ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        asyncio.run(chat())
    except KeyboardInterrupt:
        sys.exit(0)
