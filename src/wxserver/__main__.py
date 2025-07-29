import asyncio
from .server import run  # or whatever you called the async boot fn

if __name__ == "__main__":
    asyncio.run(run())
