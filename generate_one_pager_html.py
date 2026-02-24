
import os
import re

# Source and Destination
SOURCE_FILE = r'C:\Users\iLink\.gemini\antigravity\brain\f3bb4200-28f1-44d9-a362-f25d1ac5f97c\greenspoon_architecture_one_pager.md'
DEST_FILE = r'C:\Users\iLink\.gemini\antigravity\brain\f3bb4200-28f1-44d9-a362-f25d1ac5f97c\greenspoon_architecture_one_pager.html'

def convert_md_to_html_manual():
    with open(SOURCE_FILE, 'r', encoding='utf-8') as f:
        md_content = f.read()

    html_body = md_content

    # 1. Convert Headers
    html_body = re.sub(r'^# (.+)$', r'<h1>\1</h1>', html_body, flags=re.MULTILINE)
    html_body = re.sub(r'^## (.+)$', r'<h2>\1</h2>', html_body, flags=re.MULTILINE)
    html_body = re.sub(r'^### (.+)$', r'<h3>\1</h3>', html_body, flags=re.MULTILINE)

    # 2. Convert Bold
    html_body = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', html_body)

    # 3. Convert Lists
    # Simple bullet points
    html_body = re.sub(r'^\* (.+)$', r'<ul><li>\1</li></ul>', html_body, flags=re.MULTILINE)
    # Fix nested ul tags (hacky but works for visual one-pagers)
    html_body = html_body.replace('</ul>\n<ul>', '')

    # 4. Convert Mermaid Blocks
    # Regex to capture ```mermaid ... ```
    # responsive non-greedy match
    html_body = re.sub(
        r'```mermaid\s+(.+?)\s+```', 
        r'<div class="mermaid">\n\1\n</div>', 
        html_body, 
        flags=re.DOTALL
    )

    # 5. Convert Newlines to <br> or paragraphs (Simple version)
    # Just wrap lines that aren't tags in <p>? No, let's trust the browser's whitespace handling or simple replace
    # A simple way for viewability: double newline to <p>
    # (Leaving strictly as is for headers/divs)
    
    # HTML Template
    full_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Greenspoon Architecture One-Pager</title>
        <script type="module">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
            mermaid.initialize({{ startOnLoad: true, theme: 'neutral' }});
        </script>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 210mm; /* A4 width */
                margin: 0 auto;
                padding: 20mm;
                background-color: white;
            }}
            h1 {{ color: #2E7D32; border-bottom: 2px solid #2E7D32; padding-bottom: 10px; }}
            h2 {{ color: #1B5E20; margin-top: 30px; border-bottom: 1px solid #eee; }}
            h3 {{ color: #388E3C; margin-top: 20px; }}
            strong {{ color: #1B5E20; }}
            li {{ margin-bottom: 5px; }}
            .mermaid {{ margin: 30px 0; text-align: center; }}
            
            /* Print Specifics */
            @media print {{
                body {{ width: 210mm; height: 297mm; padding: 10mm; }}
                .no-print {{ display: none; }}
                h2 {{ page-break-after: avoid; }}
                img, div {{ page-break-inside: avoid; }}
            }}
        </style>
    </head>
    <body>
        <div class="no-print" style="background: #e8f5e9; padding: 10px; text-align: center; border: 1px solid #c8e6c9; margin-bottom: 20px;">
            <strong>Print Instructions:</strong> Right-click -> Print -> Save as PDF. 
            Ensure "Background Graphics" is checked in settings.
        </div>
        {html_body}
    </body>
    </html>
    """

    with open(DEST_FILE, 'w', encoding='utf-8') as f:
        f.write(full_html)
    
    print(f"Successfully created: {DEST_FILE}")

if __name__ == "__main__":
    convert_md_to_html_manual()
