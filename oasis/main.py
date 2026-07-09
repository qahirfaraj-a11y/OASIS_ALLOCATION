import flet as ft
import os
import time
from oasis.logic.order_engine import OrderEngine
from oasis.llm.inference import RuleBasedLLM, LocalLLM

# Configure Logging (centralized — honors OASIS_LOG_LEVEL/FORMAT/FILE)
import logging
from oasis.logging_config import configure_logging
configure_logging()
logger = logging.getLogger("App")

def main(page: ft.Page):
    page.title = "O.A.S.I.S. — Optimized Acquisition & Stock Intelligence System"
    page.theme_mode = ft.ThemeMode.DARK
    page.padding = 20
    
    # State
    current_file_paths = []
    engine = OrderEngine("oasis/data")
    engine.load_local_databases() # Load JSON DBs and Scan GRN files on startup
    
    # Auto-detect Local Model
    models_dir = "models"
    model_path = None
    llama_cpp_available = False
    try:
        import llama_cpp
        llama_cpp_available = True
    except ImportError:
        pass

    if os.path.exists(models_dir):
        for f in os.listdir(models_dir):
            if f.endswith(".gguf"):
                model_path = os.path.join(models_dir, f)
                break
    
    if model_path and llama_cpp_available:
        print(f"Found local model: {model_path}")
        llm = LocalLLM(model_path)
        llm.load_model()
        status_text = ft.Text(f"Ready (Local AI Active: {os.path.basename(model_path)})", color="green")
    else:
        llm = RuleBasedLLM()
        if model_path and not llama_cpp_available:
            status_text = ft.Text("Ready (Rule-Based Engine Active - Install llama-cpp-python for Local AI)", color="orange")
        else:
            status_text = ft.Text("Ready (Rule-Based Engine Active)", color="blue")
    
    def pick_files_result(e: ft.FilePickerResultEvent):
        nonlocal current_file_paths
        if e.files:
            current_file_paths = [f.path for f in e.files]
            if len(current_file_paths) == 1:
                selected_file_text.value = f"Selected: {e.files[0].name}"
            else:
                selected_file_text.value = f"Selected {len(current_file_paths)} files"
            process_btn.disabled = False
            page.update()
            
    file_picker = ft.FilePicker(on_result=pick_files_result)
    page.overlay.append(file_picker)
    
    selected_file_text = ft.Text("No file selected")
    
    # BUG 2 FIX: Mode selection via UI dropdown (was unreachable title-based detection)
    mode_dropdown = ft.Dropdown(
        label="Order Mode",
        value="replenishment",
        width=250,
        options=[
            ft.dropdown.Option("replenishment", "Replenishment (Reorder)"),
            ft.dropdown.Option("initial_load", "Initial Load (Greenfield)"),
        ]
    )
    
    results_table = ft.DataTable(
        columns=[
            ft.DataColumn(ft.Text("Product")),
            ft.DataColumn(ft.Text("Rec. Qty"), numeric=True),
            ft.DataColumn(ft.Text("Reasoning")),
        ],
        rows=[]
    )
    
    async def process_order(e):
        if not current_file_paths: return
        
        process_btn.disabled = True
        progress_ring.visible = True
        page.update()
        
        # BUG 2 FIX: Read mode from UI dropdown instead of unreachable title check
        allocation_mode = mode_dropdown.value or "replenishment"

        all_recommendations = []
        total_products = 0
        processed_files = []

        try:
            for file_path in current_file_paths:
                file_name = os.path.basename(file_path)
                status_text.value = f"Processing {file_name}..."
                page.update()
                
                timestamp = int(time.time())
                base_name, ext = os.path.splitext(file_name)
                output_filename = f"processed_{base_name}_{timestamp}{ext}"
                output_path = os.path.join(os.path.dirname(file_path), output_filename)

                # Use the unified Master Workflow which handles sync, parsing, enrichment, and merge
                recommendations = await engine.run_intelligent_analysis(
                    file_path=file_path,
                    output_path=output_path,
                    allocation_mode=allocation_mode,
                    total_budget=getattr(engine, 'total_budget', 200000.0)
                )

                all_recommendations.extend(recommendations)
                total_products += len(recommendations)
                processed_files.append(output_filename)

            # 4. Update UI with sample of all recommendations
            results_table.rows.clear()
            for rec in all_recommendations[:50]:
                results_table.rows.append(
                    ft.DataRow(cells=[
                        ft.DataCell(ft.Text(rec.get('product_name', 'Unknown'))),
                        ft.DataCell(ft.Text(str(rec.get('recommended_quantity', 0)))),
                        ft.DataCell(ft.Text(rec.get('reasoning', ''))),
                    ])
                )
            
            status_text.value = f"Done! Processed {len(current_file_paths)} files ({total_products} products).\nSaved: {', '.join(processed_files[:3])}{'...' if len(processed_files) > 3 else ''}"
            
        except Exception as ex:
            status_text.value = f"Error: {ex}"
            logger.error(ex)
        
        process_btn.disabled = False
        progress_ring.visible = False
        page.update()

    process_btn = ft.ElevatedButton(
        "Process Orders", 
        on_click=process_order, 
        disabled=True,
        icon="play_arrow"
    )
    
    progress_ring = ft.ProgressRing(visible=False)

    page.add(
        ft.Column([
            ft.Text("O.A.S.I.S. — Optimized Acquisition & Stock Intelligence System", size=30, weight="bold"),
            ft.Divider(),
            ft.Row([
                ft.ElevatedButton(
                    "Upload Picking List", 
                    icon="upload_file", 
                    # BUG 12 FIX: Filter to inventory file types only
                    on_click=lambda _: file_picker.pick_files(
                        allow_multiple=True,
                        allowed_extensions=["xlsx", "xls", "csv"]
                    )
                ),
                selected_file_text,
                mode_dropdown
            ]),
            ft.Row([process_btn, progress_ring]),
            status_text,
            ft.Divider(),
            ft.Container(
                content=ft.Column([results_table], scroll=ft.ScrollMode.ADAPTIVE, expand=True),
                expand=True
            )
        ])
    )

if __name__ == "__main__":
    ft.app(target=main)
