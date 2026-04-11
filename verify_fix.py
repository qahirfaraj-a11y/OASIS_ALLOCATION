import asyncio
import sys
import os

sys.path.append(os.getcwd())

from app.llm.inference import LocalLLM

async def test_analyze():
    print("Initializing LocalLLM with invalid path...")
    llm = LocalLLM("invalid/path/to/model.gguf")
    # Intentional failure to load model
    llm.load_model() 
    
    products = [{"product_name": "Test Product", "historical_avg_order_qty": 10}]
    
    print("Calling analyze...")
    result = await llm.analyze(products)
    
    print(f"Result type: {type(result)}")
    
    if isinstance(result, list):
        print("Success: Result is a list.")
        for item in result:
            print(item)
    else:
        print(f"Failure: Result is {type(result)} (expected list)")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(test_analyze())
