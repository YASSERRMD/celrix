import urllib.request
import json
import asyncio
import time
import sys
import os

# Add clients/python to path for importing celrix client
sys.path.append(os.path.join(os.path.dirname(__file__), '../../clients/python'))

from celrix import CelrixClient

async def test_kv(client):
    print("Testing KV operations...")
    await client.set("key1", "value1")
    val = await client.get("key1")
    assert val == "value1", f"Expected value1, got {val}"
    
    await client.del_key("key1")
    val = await client.get("key1")
    assert val is None, f"Expected None, got {val}"
    print("KV test passed ✓")

async def test_admin_api():
    print("Testing Admin API...")
    
    # Health check
    try:
        with urllib.request.urlopen('http://localhost:9090/health') as response:
            assert response.status == 200
            data = json.loads(response.read())
            assert data["status"] == "ok"
            
        # Info check
        with urllib.request.urlopen('http://localhost:9090/info') as response:
            assert response.status == 200
            data = json.loads(response.read())
            assert "version" in data
        print("Admin API test passed ✓")
    except Exception as e:
        print(f"Admin API test failed: {e}")
        raise

async def test_vector(client):
    print("Testing Vector operations...")
    # VADD v1 [0.1, 0.2, 0.3]
    # Server defaults to 1536 dimensions
    vector = [0.1] * 1536
    await client.vadd("v1", vector)
    
    # VSEARCH
    res = await client.vsearch(vector, 1)
    print(f"Vector search result: {res}")
    
    assert res is not None
    assert len(res) > 0
    assert "v1" in res # Assuming key is returned in list
    
    print("Vector test passed ✓")

async def main():
    print("Waiting for server...")
    time.sleep(2) # Give docker time to start listening
    
    client = CelrixClient(port=6380)
    await client.connect()
    
    try:
        await test_kv(client)
        # await test_admin_api()
        await test_vector(client)
        print("\nAll integration tests passed! 🚀")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
