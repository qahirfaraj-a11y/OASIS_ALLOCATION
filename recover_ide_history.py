import json
import os
import glob
import shutil
import urllib.parse

history_dir = r"C:\Users\iLink\AppData\Roaming\Antigravity\User\History"
scratch_dir = r"C:\Users\iLink\.gemini\antigravity\scratch"
restored_count = 0

for entry_file in glob.glob(os.path.join(history_dir, "*", "entries.json")):
    try:
        with open(entry_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        resource = data.get("resource", "")
        # only look for our scratch files
        if "antigravity/scratch/" in resource and resource.endswith(".py"):
            # decode URL encoding (%3A, %20, etc)
            resource = urllib.parse.unquote(resource)
            rel_path = resource.split("antigravity/scratch/")[1]
            
            # handle cases where history might think it's 'app' instead of 'oasis'
            if rel_path.startswith("app/"):
                rel_path = "oasis/" + rel_path[4:]
            
            # fix backslashes
            rel_path = rel_path.replace("/", os.sep)
            dest_path = os.path.join(scratch_dir, rel_path)
            
            # skip files that are already populated
            if os.path.exists(dest_path) and os.path.getsize(dest_path) > 10:
                continue

            entries = data.get("entries", [])
            restored = False
            for entry in reversed(entries):
                backup_id = entry.get("id")
                if not backup_id:
                    continue
                backup_path = os.path.join(os.path.dirname(entry_file), backup_id)
                if os.path.exists(backup_path) and os.path.getsize(backup_path) > 10:
                    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                    shutil.copy2(backup_path, dest_path)
                    print(f"Restored {rel_path}")
                    restored_count += 1
                    restored = True
                    break
            
            if not restored and os.path.exists(dest_path) and os.path.getsize(dest_path) == 0:
                print(f"Could not find valid backup for empty file {rel_path}!")

    except Exception as e:
        pass

print(f"Finished. Restored {restored_count} files.")
