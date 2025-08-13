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
    "resolve_stock_industry": MSSQL_SERVER,
    "list_stocks_by_industry": MSSQL_SERVER,
    "resolve_stock_name_mssql": MSSQL_SERVER,
}

# 🔍 A — sanity-check once at import time
assert TOOL_ROUTE["query_sql_mssql"] is MSSQL_SERVER, "Router mis-mapped!"

# ── helper to call any MCP tool ─────────────────────────────────────────────
async def run_tool(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    srv = TOOL_ROUTE[name]

    # Show DSN for sanity
    print("DEBUG env.MSSQL_DSN →", os.getenv("MSSQL_DSN"), file=sys.stderr)

    async with stdio_client(srv) as (r, w):
        async with ClientSession(r, w) as sess:
            await sess.initialize()
            res = await sess.call_tool(name, args)

    # Try to surface the exact SQL that was executed if the server included it
    sc = res.structuredContent  # type: ignore[attr-defined]
    executed_sql = None
    if isinstance(sc, dict):
        executed_sql = sc.get("sql")
        if not executed_sql and isinstance(sc.get("result"), dict):
            executed_sql = sc["result"].get("sql")

    if executed_sql:
        print("DEBUG Executed SQL (from server) →\n" + executed_sql, file=sys.stderr)

    # replace the preview block with this:
    try:
        pretty = json.dumps(sc, ensure_ascii=False, indent=2)
    except Exception:
        pretty = str(sc)

    print("DEBUG result →\n" + pretty, file=sys.stderr)

    return sc



# ── function schemas for Azure (2 tools) ────────────────────────────────────
tool_schemas: List[Dict[str, Any]] = [
    {
    "name": "query_sql_mssql",
    "description": "Run a **read-only** T-SQL query and return rows as JSON."
    "**STRICT RULES (MSSQL dialect only):**\n• **Allowed statements:** SELECT / WITH only. (No INSERT/UPDATE/DELETE/MERGE/ALTER/DROP/EXEC/sp_*)"
    "• ALWAYS call `read_schema_csv` before `query_sql_mssql`"
    "• ALWAYS call `read_schema_csv` before `query_sql_mssql`"
    "• **No `LIMIT`.** Use **`TOP (N)`** or **`ORDER BY ... OFFSET 0 ROWS FETCH NEXT N ROWS ONLY`**.",
    "parameters": {
        "type": "object",
        "properties": {
        "sql": {
            "type": "string",
            "description": "A single **T-SQL** SELECT/WITH statement that obeys the rules above."
        },
        "limit": {
            "type": "integer",
            "default": 500,
            "description": "Soft cap. If your query has no TOP/FETCH, the tool will auto-limit to this."
        }
        },
        "required": ["sql"]
    }
    },
    {
    "name": "read_schema_csv",
    "description": "Return the stTseStkPrcD schema as JSON rows. DO NOT input file paths, use the default."
    "ALWAYS RUN THIS BEFORE `query_sql_mssql` to get the schema for stTseStkPrcD.",
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
    {
    "name": "resolve_stock_name_mssql",
    "description": "Return the primary company name ('name') for a given internal stock id from stScuSecuBasC.",
    "parameters": {
        "type": "object",
        "properties": {
            "stock_id": {
                "type": ["integer", "string"],
                "description": "Internal id from stScuSecuBasC (e.g., 2330)"
            }
        },
        "required": ["stock_id"]
    }
    },
    {
    "name": "resolve_stock_industry",
    "description": "Resolve an industry keyword to internal industry ids from misc.dbo.mtIndustryC for two markets. After calling this tool, ALWAYS inspect both keys: for each non-null `id`, call `list_stocks_by_industry` (once per id), merge the resulting stock ids, and use the COMBINED set in subsequent SQL filters. If only one market is present, just use that one.",
    "parameters": {
        "type": "object",
        "properties": {
        "keyword": { "type": "string", "description": "Industry keyword, e.g. '半導體' or '水泥'." }
        },
        "required": ["keyword"]
    }
    },
    {
    "name": "list_stocks_by_industry",
    "description": "Return all stocks id that belong to a given industry_id using stock.dbo.stScuSecuBasC.",
    "parameters": {
        "type": "object",
        "properties": {
            "industry_id": {
                "type": "integer",
                "description": "The numeric id from mtIndustryC.id"
            }
        },
        "required": ["industry_id"]
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
                print(f"🔧 calling {name}{args}", file=sys.stderr)

                # NEW: print full SQL we’re about to send (if this is the MSSQL tool)
                if name == "query_sql_mssql":
                    sql_txt = args.get("sql", "")
                    print("DEBUG SQL to server →\n" + sql_txt, file=sys.stderr)

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

SYSTEM_PROMPT = f"""
You are a **bilingual (繁體中文 / English) financial-data assistant** connected to a Microsoft SQL Server data-warehouse.
Current calendar date (Asia/Taipei): {today_taipei}
Your mission:
• Help users explore Taiwanese stock fundamentals and prices.

**Instructions**:
• When a user mentions a company name or market id, first translate it to an internal stock id using `resolve_stock_id_mssql`.
• NEVER execute INSERT/UPDATE/DELETE.
• If a query would return more than 500 rows, aggregate or LIMIT 100.
• If data is unavailable, say so and suggest an alternative metric.
• ALWAYS call `read_schema_csv` before `query_sql_mssql` for a table.
• If a user asks for data from a month or year, always query the full month/year, not just the current date or last day.
"""


# ── main ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        asyncio.run(chat())
    except KeyboardInterrupt:
        sys.exit(0)
