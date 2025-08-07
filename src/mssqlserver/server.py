# src/mssqlserver/server.py
"""
Minimal MCP server for Microsoft SQL Server.
• One tool  : query_sql(sql, limit=500)
• Transport : stdio (FastMCP)
Env vars required (.env or shell):
    MSSQL_DSN  = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:host,1433;\
                  Database=mydb;UID=user;PWD=pw;Encrypt=yes;TrustServerCertificate=yes"
"""
import csv
from pathlib import Path
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

_SCHEMA_CSV = Path(__file__).with_name("stTseStkPrcD_schema.csv")

@mcp.tool()
async def read_schema_csv(file: str | None = None) -> List[Dict[str, str]]:
    """
    Load the column-definition CSV and return it as a JSON array.

    Parameters
    ----------
    file : str | None
        Optional custom path.  If omitted, uses 'stTseStkPrcD_schema.csv'
        located in the same directory as server.py.

    Returns
    -------
    list[dict]
        Each row is a dict with keys:
        'Column_name', 'Explanation', 'Datatype', 'Availability'
    """
    path = _SCHEMA_CSV
    if not path.exists():
        raise FileNotFoundError(f"CSV not found → {path}")

    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)

# ---------------------------------------------------------------------------
# 🔎  Resolve stock ID from common name / alias  -----------------------------
# ---------------------------------------------------------------------------
@mcp.tool()
async def resolve_stock_id_mssql(keyword: str) -> Dict[str, Any]:
    """
    Resolve the internal `id` from stScuSecuBasC with this priority:

    1. Exact match on listCode             (e.g. '2330')
    2. Exact match on any alias column     – but ignoring all spaces
    3. Fallback: LIKE '%keyword%'          – also ignoring spaces
    """
    pool = await get_pool()
    kw_raw = keyword.strip()

    # Strip ASCII space, full-width space (U+3000) and tabs
    kw_ns = kw_raw.replace(" ", "").replace("\u3000", "").replace("\t", "")

    async with pool.acquire() as conn, conn.cursor() as cur:

        # 1️⃣ exact listCode --------------------------------------------------
        await cur.execute(
            """SELECT id, 'listCode' AS matched_column, listCode AS matched_value
               FROM   stScuSecuBasC
               WHERE  listCode = ?""",
            (kw_raw,),
        )
        if (row := await cur.fetchone()):
            return dict(zip([c[0] for c in cur.description], row))

        # Helper expression to strip spaces in SQL
        def nosp(col: str) -> str:
            return (
                "REPLACE(REPLACE(REPLACE(" + col +
                ", N' ', N''), NCHAR(12288), N''), CHAR(9), N'')"
            )

        # 2️⃣ exact alias (space-insensitive) ----------------------------------
        exact_sql = f"""
        DECLARE @kw NVARCHAR(100) = ?;

        SELECT TOP (1) id, colname, alias
        FROM (
            SELECT id, 'name'        AS colname, name        AS alias, {nosp('name')}        AS alias_ns FROM stScuSecuBasC
            UNION ALL
            SELECT id, 'name3',      name3      AS alias, {nosp('name3')}      FROM stScuSecuBasC
            UNION ALL
            SELECT id, 'name4',      name4      AS alias, {nosp('name4')}      FROM stScuSecuBasC
            UNION ALL
            SELECT id, 'nameV2',     nameV2     AS alias, {nosp('nameV2')}     FROM stScuSecuBasC
            UNION ALL
            SELECT id, 'nameAbbrV2', nameAbbrV2 AS alias, {nosp('nameAbbrV2')} FROM stScuSecuBasC
        ) AS u
        WHERE alias_ns = @kw;
        """
        await cur.execute(exact_sql, (kw_ns,))
        if (row := await cur.fetchone()):
            return {
                "id":             row[0],
                "matched_column": row[1],
                "matched_value":  row[2].strip(),
            }

        # 3️⃣ LIKE '%kw%' fallback (space-insensitive) ------------------------
        like_sql = exact_sql.replace("= @kw", "LIKE '%' + @kw + '%'")
        await cur.execute(like_sql, (kw_ns,))
        if (row := await cur.fetchone()):
            return {
                "id":             row[0],
                "matched_column": row[1],
                "matched_value":  row[2].strip(),
            }

    # nothing found -----------------------------------------------------------
    return {}

# ---------------------------------------------------------------------------
# 🔎  Resolve industry id from name / Eng / Abbr  ----------------------------
# ---------------------------------------------------------------------------
@mcp.tool()
async def resolve_stock_industry(keyword: str) -> dict[str, Any]:
    """
    Translate an industry keyword to its internal `id` (misc.dbo.mtIndustryC).

    Priority
    --------
    1. Exact match on any alias column (name / nameEng / nameAbbr)
    2. Fallback: LIKE '%keyword%' ordered by shortest alias

    Returns {"id": 42, "matched_column": "name", "matched_value": "水泥"} or {}
    """
    pool = await get_pool()                   # same DSN, same login
    kw = keyword.strip()

    async with pool.acquire() as conn, conn.cursor() as cur:

        # 1️⃣  exact match ----------------------------------------------------
        exact_sql = """
        SELECT TOP (1) id, colname, alias
        FROM (
            SELECT id, 'name'      AS colname, name      AS alias FROM misc.dbo.mtIndustryC WHERE name      IS NOT NULL
            UNION ALL
            SELECT id, 'nameEng'   AS colname, nameEng   AS alias FROM misc.dbo.mtIndustryC WHERE nameEng   IS NOT NULL
            UNION ALL
            SELECT id, 'nameAbbr'  AS colname, nameAbbr  AS alias FROM misc.dbo.mtIndustryC WHERE nameAbbr  IS NOT NULL
        ) AS u
        WHERE alias = ?
        """
        await cur.execute(exact_sql, (kw,))
        row = await cur.fetchone()
        if row:
            return {"id": row[0], "matched_column": row[1], "matched_value": row[2].strip()}

        # 2️⃣  LIKE '%kw%' fallback ------------------------------------------
        like_sql = exact_sql + """
        ORDER BY LEN(alias), alias   -- shortest alias first
        """
        await cur.execute(like_sql.replace("alias = ?", "alias LIKE ?"), (f"%{kw}%",))
        row = await cur.fetchone()
        if row:
            return {"id": row[0], "matched_column": row[1], "matched_value": row[2].strip()}

    # nothing found
    return {}

# ---------------------------------------------------------------------------
# 🗂️  List all stocks in an industry  ----------------------------------------
# ---------------------------------------------------------------------------
@mcp.tool()
async def list_stocks_by_industry(industry_id: int) -> list[dict[str, Any]]:
    """
    Given an `industry_id` ( = mtIndustryC_id ),
    return all matching stocks from **stock.dbo.stScuSecuBasC**.

    Each row -> {"id": 748, "listCode": "2330", "nameAbbrV2": "台積電"}
    """
    pool = await get_pool()
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            SELECT id, listCode, nameAbbrV2
            FROM   stock.dbo.stScuSecuBasC
            WHERE  mtIndustryC_id = ?
            ORDER  BY id
            """,
            (industry_id,),
        )
        cols = [c[0] for c in cur.description]      # ["id", "listCode", …]
        rows = await cur.fetchall()
        return [dict(zip(cols, r)) for r in rows]

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


