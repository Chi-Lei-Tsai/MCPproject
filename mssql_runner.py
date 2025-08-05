"""
mcp_runner.py – routes Azure OpenAI function calls to two MCP servers:
  • pgserver   – Postgres read‑only SQL tools
  • wxserver   – demo calculator with an `add` tool
Run: uv run python mcp_runner.py
"""

import asyncio, json, os, sys
from typing import Any, Dict, List, cast
from datetime import datetime, timezone, timedelta
today_taipei = datetime.now(timezone(timedelta(hours=8))).date()
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
    "read_schema_csv":      MSSQL_SERVER,
    "resolve_stock_id_mssql": MSSQL_SERVER,
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


# ── function schemas for Azure (2 tools) ────────────────────────────────────
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
    },
    {
    "name": "read_schema_csv",
    "description": "Return the stTseStkPrcd schema as JSON rows.",
    "parameters": {
        "type": "object",
        "properties": {
        "file": { "type": "string", "description": "Optional custom CSV path" }
        }
    }
    },
    {
    "name": "resolve_stock_id_mssql",
    "description": "Translate a company name / abbreviation / listCode to its internal id using stScuSecuBasC.",
    "parameters": {
        "type": "object",
        "properties": {
            "keyword": {"type": "string", "description": "e.g. '台積電' or '2330'"}
        },
        "required": ["keyword"]
    }
    },
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
You are a **bilingual (繁體中文 / English) financial-data assistant** connected to a Microsoft SQL Server data-warehouse.
Current calendar date (Asia/Taipei): {today_taipei}
## Your mission

* Help users explore Taiwanese equities: prices, volumes, market-value, valuation ratios, etc.

## Workflow you MUST follow

1. **Identify the data**
   *When a user references a company name / abbreviation / listCode, call **`resolve_stock_id_mssql`** with the raw text.
   If no match is found, politely ask for clarification.

2. **Query column definitions**
   Use **`read_schema_csv`** to read the `stTseStkPrcD_schema.csv` file.
   This file contains the column names and their meanings, of the `stTseStkPrcD` table, 
   which contains daily stock prices and other market data.

3. **Query market data**
   Use **`query_sql_mssql`** to read `stTseStkPrcD`.
   *Important SQL rules*

   * read-only: **SELECT / WITH only** – never attempt INSERT/UPDATE/DELETE.
   * Column names are case-sensitive; wrap identifiers that contain capitals in square-brackets, e.g. `[AskPrice1]`.
   * If your query might return a very large result set, add `TOP(n)` or a proper `ORDER BY … OFFSET … FETCH NEXT` clause to keep rows ≤ 500.

4. **Explain & format**

   * For numerical answers include both the figure and its unit (e.g. “成交量 23 萬張”).
   * When you present a stock, always prefix the 4-digit id and a representative alias, e.g. **“2330 台積電”**.
   * Provide crisp, actionable explanations; show calculations when useful; switch freely between Chinese and English depending on the user’s language.

## Safety & etiquette

* Never guess an `id`; always rely on the resolver tool.
* If data is missing or the query returns no rows, say so and suggest an alternative (e.g., previous closing price).
"""



# ── main ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        asyncio.run(chat())
    except KeyboardInterrupt:
        sys.exit(0)
