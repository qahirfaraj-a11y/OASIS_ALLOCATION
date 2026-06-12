import os
import asyncio
from app.logic.order_engine import OrderEngine
from app.llm.inference import RuleBasedLLM

async def test_multi_file_logic():
    engine = OrderEngine("app/data")
    engine.load_local_databases()
    llm = RuleBasedLLM()
    
    # Use existing sample file
    sample_file = "sample_picking_list.xlsx"
    if not os.path.exists(sample_file):
        print(f"Sample file {sample_file} not found. Creating a dummy one...")
        # Create a dummy excel file if needed, but we should have one in scratch
        return

    file_paths = [sample_file, sample_file] # Simulate two files
    
    print(f"Testing with {len(file_paths)} files...")
    
    all_recommendations = []
    for i, file_path in enumerate(file_paths):
        print(f"Processing file {i+1}: {file_path}")
        products = engine.parse_inventory_file(file_path)
        products = engine.enrich_product_data(products)
        recommendations = await llm.analyze(products)
        all_recommendations.extend(recommendations)
        
        output_filename = f"test_output_{i}.xlsx"
        engine.generate_excel_report(file_path, recommendations, output_filename)
        print(f"Generated {output_filename}")
        
    print(f"Total recommendations: {len(all_recommendations)}")
    print("Verification complete.")

if __name__ == "__main__":
    asyncio.run(test_multi_file_logic())
