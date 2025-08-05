# src/mssqlserver/server.py
"""
Minimal MCP server for Microsoft SQL Server.
• One tool  : query_sql(sql, limit=500)
• Transport : stdio (FastMCP)
Env vars required (.env or shell):
    MSSQL_DSN  = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:host,1433;\
                  Database=mydb;UID=user;PWD=pw;Encrypt=yes;TrustServerCertificate=yes"
"""

import asyncio, os
from typing import Any, Dict, List

import aioodbc
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

DSN = os.environ["MSSQL_DSN"]           # raise KeyError if missing
POOL_MIN, POOL_MAX = 1, 5

mcp  = FastMCP("mssql-demo")
_pool: aioodbc.pool.Pool | None = None


async def get_pool() -> aioodbc.pool.Pool:
    """Singleton aioodbc pool (lazy init)."""
    global _pool
    if _pool is None:
        _pool = await aioodbc.create_pool(
            dsn=DSN, minsize=POOL_MIN, maxsize=POOL_MAX, autocommit=True
        )
    return _pool


@mcp.tool()
async def query_sql_mssql(sql: str, limit: int = 500) -> List[Dict[str, Any]] | Dict[str, str]:
    """
    Run a **read-only** T-SQL statement (SELECT / WITH).
    If the caller forgets to limit rows, append
        OFFSET 0 ROWS FETCH NEXT <limit> ROWS ONLY
    so we never return an unbounded result.
    On ANY exception, write the SQL + traceback to stderr and
    return {"error": "...", "sql": "<the-sql>"} instead of null.
    """
    import sys, traceback

    low = sql.strip().lower()
    if not low.startswith(("select", "with")):
        raise ValueError("Only SELECT / WITH statements are allowed")

    # auto-limit rows if caller didn't
    import re
    if (" top " not in low
            and " fetch next " not in low
            and " order by " not in low):
        # add TOP when there's no existing limit and no ORDER BY
        sql = re.sub(r"^\s*select\b", f"SELECT TOP ({limit})", sql, count=1, flags=re.I)
    elif " fetch next " not in low and " order by " in low:
        # has ORDER BY but no FETCH ⇒ append fetch
        sql += f" OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"

    try:
        pool = await get_pool()
        async with pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = await cur.fetchall()
            return [dict(zip(cols, r)) for r in rows]

    except Exception as e:
        # --- dump details to server stderr (shows in Claude / runner log) ---
        print("\n=== SQL ERROR in query_sql_mssql ===", file=sys.stderr)
        print(sql, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

        # --- bubble a JSON error payload back to the host ---------------
        return {"error": str(e), "sql": sql}



# ── BOOT ──────────────────────────────────────────────────────────────
async def run() -> None:
    """Entry-point used by  python -m mssqlserver  (stdio)"""
    await mcp.run_stdio_async()          # FastMCP handles JSON-RPC loop


async def main():
    # single line — FastMCP handles stdio internally
    await mcp.run_stdio_async()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


