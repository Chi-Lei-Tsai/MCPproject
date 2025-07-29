# src/pgserver/server.py
import asyncio, os
from typing import Any, Dict, List

import asyncpg
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
import mcp.server.stdio as stdio

load_dotenv()

DB_URL = (
    f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
    f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
)

mcp = FastMCP("pg-demo")

_pool: asyncpg.Pool | None = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=DB_URL, min_size=1, max_size=5)
    return _pool

@mcp.tool()
async def resolve_stock_id(name: str) -> Dict[str, str]:
    pool = await get_pool()
    q = """
        SELECT stock_id, alias AS match
        FROM stock_alias
        WHERE stock_id = $1
           OR alias ILIKE $1
        ORDER BY similarity(alias, $1) DESC
        LIMIT 1;
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(q, name)
        return dict(row) if row else {}

@mcp.tool()
async def resolve_stock_name(stock_id: str) -> dict[str, str]:
    """
    Given a 4‑digit numeric stock_id, return a *representative* company name.

    Strategy:
    • Pick the shortest alias for that stock_id
      (these are usually the common brand names: 台積電, 鴻海…).
    • If there are several with the same length, pick the alphabetically first.

    Returns {"stock_id": "2330", "name": "台積電"} or {} if not found.
    """
    pool = await get_pool()
    q = """
        SELECT stock_id,
               alias      AS name
        FROM   stock_alias
        WHERE  stock_id = $1
        ORDER  BY length(alias), alias           -- shortest / tie‑break
        LIMIT  1;
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(q, stock_id)
        return dict(row) if row else {}
   
@mcp.tool()
async def query_sql(sql: str, limit: int = 500) -> List[Dict[str, Any]]:
    """Run a read-only SELECT/WITH/EXPLAIN query."""
    if not sql.strip().lower().startswith(("select", "with", "explain")):
        raise ValueError("Only SELECT/WITH/EXPLAIN allowed.")
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql)
        return [dict(r) for r in rows[:limit]]

@mcp.tool()
async def list_tables(schema: str = "public") -> List[str]:
    q = """SELECT table_name
           FROM information_schema.tables
           WHERE table_schema = $1
           ORDER BY table_name;"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return [r["table_name"] for r in await conn.fetch(q, schema)]

@mcp.tool()
async def describe_table(table: str, schema: str = "public") -> List[Dict[str, Any]]:
    """
    Return column metadata for a table, ordered by ordinal_position.
    Each row: {"column_name": "close", "data_type": "numeric", "is_nullable": "NO"}
    """
    q = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position;
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(q, schema, table)
        return [dict(r) for r in rows]

# ... (imports, tools, get_pool, etc. stay the same)

async def main():
    # single line — FastMCP handles stdio internally
    await mcp.run_stdio_async()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

