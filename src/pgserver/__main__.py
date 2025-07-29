import asyncio
from .server import main   # main() is the async boot function we wrote

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
