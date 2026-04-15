import os
import re

def process_book(input_file, output_dir):
    with open(input_file, "r", encoding="utf-8") as f:
        text = f.read()

    # Split by Chapter headings that start a line
    # Format usually is "Chapter 1: Title..."
    chapters = []
    
    # Simple regex to split the text by Chapter headings
    pattern = re.compile(r"^(Chapter \d+:\s*(.*?))(\n|$)", re.MULTILINE)
    
    matches = list(pattern.finditer(text))
    
    if not matches:
        print("No chapters found. Please check regex.")
        return
        
    toc_lines = ['---', 'type: TOC', 'title: "Kenyan Retail Bible"', '---', '# 📖 The Kenyan Retail Bible \n\n*A Masterclass in High-Volume Logistics, Neural Networks, and Capital Preservation.*\n\n## Table of Contents\n']
    
    for i in range(len(matches)):
        start = matches[i].start()
        end = matches[i+1].start() if i + 1 < len(matches) else len(text)
        
        chapter_title_full = matches[i].group(1).strip()
        # Find chapter num
        num_match = re.search(r"Chapter (\d+):", chapter_title_full)
        if not num_match:
            continue
            
        chapter_num = int(num_match.group(1))
        
        # Clean title for filename 
        clean_title = re.sub(r"[^\w\s-]", "", chapter_title_full).replace(" ", "_")
        
        # Limit filename length and format
        # e.g. Chapter_1_The_Retail_Continuum
        short_title = clean_title.split("_The_Physics")[0].split("_Why_Variety")[0].split("_Executing_the")[0].split("_The_Algorithmic")[0] 
        
        filename = f"{short_title}.md"
        
        # Extract content
        content = text[matches[i].end():end].strip()
        
        # Add to TOC
        toc_lines.append(f"### [[{short_title}]]\n* {chapter_title_full}\n")
        
        # Build Markdown file
        md_content = f"""---
type: Chapter
chapter: {chapter_num}
title: "{chapter_title_full}"
---
# {chapter_title_full}

{content}
"""
        with open(os.path.join(output_dir, filename), "w", encoding="utf-8") as out_f:
            out_f.write(md_content)
            print(f"Written: {filename}")
            
    # Write TOC
    with open(os.path.join(output_dir, "00_TOC.md"), "w", encoding="utf-8") as toc_f:
        toc_f.write("\n".join(toc_lines))
        print("Written: 00_TOC.md")

if __name__ == "__main__":
    input_path = r"c:\Users\iLink\.gemini\antigravity\scratch\Algorithmic_retail_extracted.txt"
    out_path = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis_vault\Kenyan_Retail_Bible"
    
    os.makedirs(out_path, exist_ok=True)
    process_book(input_path, out_path)
