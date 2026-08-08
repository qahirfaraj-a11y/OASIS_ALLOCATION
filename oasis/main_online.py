import flet as ft
from oasis.logic.order_engine import OrderEngine
import asyncio
import os

# Create a temporary upload directory
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), 'uploads')
os.makedirs(UPLOAD_DIR, exist_ok=True)

def main(page: ft.Page):
    page.title = "OASIS Smart Ordering - Online Terminal"
    page.theme_mode = "dark"
    
    # Initialization
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    engine = OrderEngine(data_dir)
    
    status_text = ft.Text("Ready to process orders...")
    selected_file_text = ft.Text("No file selected")
    current_file_paths = []
    
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
        
        all_recommendations = []
        total_products = 0

        try:
            for file_path in current_file_paths:
                file_name = os.path.basename(file_path)
                status_text.value = f"Processing {file_name}..."
                page.update()
                
                # 1. Parse
                products = engine.parse_inventory_file(file_path)
                status_text.value = f"Parsed {len(products)} products from {file_name}. Enriching..."
                page.update()
                
                # 2. Enrich
                products = engine.enrich_product_data(products)
                
                # 3. Smart Rules Analysis
                status_text.value = f"Running Smart Ordering Rules for {file_name}..."
                page.update()
                
                from oasis.logic.simulation_bridge import SimulationOrderUtil
                from datetime import datetime
                
                def run_smart_logic(prods):
                    sim_util = SimulationOrderUtil(data_dir)
                    current_yday = datetime.now().timetuple().tm_yday
                    raw = sim_util.calculate_order_quantity(prods, current_day=current_yday)
                    return sim_util.finalize_orders(raw)
                    
                recommendations = await asyncio.to_thread(run_smart_logic, products)
                all_recommendations.extend(recommendations)
                total_products += len(products)
                
            # 4. Update UI with sample of recommendations
            results_table.rows.clear()
            for rec in all_recommendations[:100]:
                results_table.rows.append(
                    ft.DataRow(cells=[
                        ft.DataCell(ft.Text(rec.get('product_name', 'Unknown'))),
                        ft.DataCell(ft.Text(str(rec.get('recommended_quantity', 0)))),
                        ft.DataCell(ft.Text(rec.get('reasoning', ''))),
                    ])
                )
            
            status_text.value = f"Done! Processed {len(current_file_paths)} files ({total_products} products)."
            
        except Exception as ex:
            status_text.value = f"Error: {ex}"
            import traceback
            traceback.print_exc()
        
        process_btn.disabled = False
        progress_ring.visible = False
        page.update()

    def on_upload_progress(e: ft.FilePickerUploadEvent):
        if e.progress == 1:
            status_text.value = f"Uploaded {e.file_name}"
            # The file is saved directly to UPLOAD_DIR by Flet
            filepath = os.path.join(UPLOAD_DIR, e.file_name)
            if filepath not in current_file_paths:
                current_file_paths.append(filepath)
            
            selected_file_text.value = f"Ready: {e.file_name}"
            process_btn.disabled = False
            page.update()
        else:
            status_text.value = f"Uploading: {e.progress * 100:.0f}%"
            page.update()
            
    def on_dialog_result(e: ft.FilePickerResultEvent):
        nonlocal current_file_paths
        if e.files:
            current_file_paths = []
            selected_file_text.value = f"Uploading {len(e.files)} files..."
            status_text.value = "Starting upload..."
            page.update()
            
            # Start actual uploads to the server using standard upload logic
            upload_list = []
            for f in e.files:
                upload_list.append(ft.FilePickerUploadFile(
                    f.name,
                    upload_url=page.get_upload_url(f.name, 60)
                ))
            file_picker.upload(upload_list)
    
    file_picker = ft.FilePicker(on_result=on_dialog_result, on_upload=on_upload_progress)
    page.overlay.append(file_picker)
    
    process_btn = ft.ElevatedButton(
        "Process Orders", 
        on_click=process_order, 
        disabled=True,
        icon="play_arrow"
    )
    progress_ring = ft.ProgressRing(visible=False)

    page.add(
        ft.Column([
            ft.Text("O.A.S.I.S. — Online Terminal Mode", size=30, weight="bold"),
            ft.Text("Use this interface to generate order recommendations from anywhere.", size=14),
            ft.Divider(),
            ft.Row([
                ft.ElevatedButton(
                    "Upload Picking List CSV", 
                    icon="upload_file", 
                    on_click=lambda _: file_picker.pick_files(allow_multiple=True)
                ),
                selected_file_text
            ]),
            ft.Row([process_btn, progress_ring]),
            status_text,
            ft.Divider(),
            ft.Container(
                content=ft.Column([results_table], scroll=ft.ScrollMode.ADAPTIVE),
                expand=True
            )
        ], expand=True)
    )

import socket

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"

if __name__ == "__main__":
    import os as _os
    import sys as _sys
    # 8550 is claimed by --mode api; default to a free port and allow an
    # explicit override (OASIS_ONLINE_PORT or argv[1]).
    port = int(_os.environ.get("OASIS_ONLINE_PORT", 8555))
    if len(_sys.argv) > 1:
        port = int(_sys.argv[1])
    local_ip = get_local_ip()
    
    print("==================================================")
    print("  OASIS IS ONLINE ON YOUR LOCAL NETWORK!  ")
    print("  To access from your phone or another computer:  ")
    print(f"  Go to http://{local_ip}:{port}  ")
    print("==================================================")
    
    ft.app(
        target=main, 
        view=ft.AppView.WEB_BROWSER, 
        host="0.0.0.0",
        port=port,
        upload_dir=UPLOAD_DIR
    )
