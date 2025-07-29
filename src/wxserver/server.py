import asyncio
from mcp.server.lowlevel import Server, NotificationOptions
import mcp.server.stdio as stdio
import mcp.types as types
from mcp.server.models import InitializationOptions

server = Server("mini‑example")

# -------- Resources --------
@server.list_resources()
async def list_resources() -> list[types.Resource]:
    return [
        types.Resource(
            uri="greeting://World",
            title="Hello World greeting",
            description="A tiny static resource",
            contentType="text/plain",
        )
    ]

@server.read_resource()
async def read_resource(uri: str) -> types.ReadResourceResult:
    if uri != "greeting://World":
        raise ValueError("Unknown resource")
    return types.ReadResourceResult(
        contents=[types.TextContent(type="text", text="👋 Hello, World!")]
    )

# -------- Tools --------
@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="add",
            description="Add two numbers",
            inputSchema={
                "type": "object",
                "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
                "required": ["a", "b"],
            },
            outputSchema={
                "type": "object",
                "properties": {"result": {"type": "number"}},
                "required": ["result"],
            },
        )
    ]

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> dict:
    if name != "add":
        raise ValueError("Unknown tool")
    return {"result": arguments["a"] + arguments["b"]}

# -------- Boot --------
async def run() -> None:
    async with stdio.stdio_server() as (r, w):
        await server.run(
            r,
            w,
            InitializationOptions(
                server_name="mini‑example",
                server_version="0.1.0",
                capabilities = server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},          # 👈 new
                )
            ),
        )

if __name__ == "__main__":
    asyncio.run(run())
