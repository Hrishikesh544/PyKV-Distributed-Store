import asyncio

async def say_hello():
    print("Hello...")
    await asyncio.sleep(2)
    print("World!")

asyncio.run(say_hello())
