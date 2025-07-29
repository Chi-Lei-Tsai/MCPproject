# testing.py  – bulk‑load alias.csv into stock_alias
import asyncio, csv, asyncpg, os
from dotenv import load_dotenv

load_dotenv()                       # make sure .env is read

DB_URL = (
    f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
    f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
)

async def bulk_load(path="alias.csv"):
    pool = await asyncpg.create_pool(dsn=DB_URL)

    async with pool.acquire() as conn:
        with open(path, encoding="utf-8") as f:        # sync context inside
            reader = csv.reader(f)
            next(reader)                               # skip header
            await conn.execute("TRUNCATE stock_alias") # optional
            await conn.copy_records_to_table(
                "stock_alias",
                columns=("stock_id", "alias"),
                records=reader,
            )

    await pool.close()
    print("✅ loaded aliases")


if __name__ == "__main__":
    asyncio.run(bulk_load("alias.csv"))
