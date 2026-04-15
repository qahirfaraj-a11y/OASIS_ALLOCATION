import os
import shutil
import glob
import argparse

# Configuration
SCRATCH_DIR = os.path.abspath(os.path.dirname(__file__))
ARCHIVE_DIR = os.path.abspath(os.path.join(SCRATCH_DIR, "..", "simulations"))

# Critical Protection: NEVER move or delete these
PROTECTED_EXTENSIONS = {'.py', '.bat', '.db', '.json', '.pt', '.md', '.env', '.gitignore', '.dockerignore'}
PROTECTED_FILES = {'requirements.txt', 'Dockerfile', 'docker-compose.yml', 'pyrightconfig.json'}
PROTECTED_DIRS = {'.git', '.vscode', '.oasis_venv', 'oasis/data'}

# Targeted Patterns
ARCHIVE_PATTERNS = [
    "simulation_results_*.xlsx",
    "simulation_metrics_*.csv",
    "orders_*.csv",
    "allocation_gap_analysis_*.xlsx",
    "verified_recommendations.xlsx"
]

DELETE_PATTERNS = [
    "__pycache__",
    ".pytest_cache",
    "build",
    "dist",
    "*.pyc",
    "*.pyo",
    "*.pyd",
    "*.db-shm",
    "*.db-wal",
    "*.db-journal",
    "debug_output_*.txt",
    "report_output.txt",
    "pyflakes*.log",
    "py_diff.txt",
    "diff_*.txt",
    "rhapta_analysis_log.txt"
]

def is_protected(path):
    rel_path = os.path.relpath(path, SCRATCH_DIR)
    
    # Check directory protection
    for pd in PROTECTED_DIRS:
        if rel_path.startswith(pd) or rel_path == pd:
            return True
            
    # Check file protection
    name = os.path.basename(path)
    ext = os.path.splitext(name)[1].lower()
    
    if name in PROTECTED_FILES:
        return True
    if ext in PROTECTED_EXTENSIONS:
        # Extra safeguard: ensure we don't delete master data files
        return True
        
    return False

def run_archival(dry_run=False):
    print(f"\n--- Phase 1: Archival to {ARCHIVE_DIR} ---")
    if not os.path.exists(ARCHIVE_DIR) and not dry_run:
        os.makedirs(ARCHIVE_DIR)
        print(f"Created archive directory: {ARCHIVE_DIR}")

    moved_count = 0
    for pattern in ARCHIVE_PATTERNS:
        # Archival only in root scratch dir
        for path in glob.glob(os.path.join(SCRATCH_DIR, pattern)):
            if is_protected(path) or os.path.isdir(path):
                continue
            
            dest = os.path.join(ARCHIVE_DIR, os.path.basename(path))
            print(f"{'[DRY RUN] ' if dry_run else ''}Moving: {os.path.basename(path)} -> simulations/")
            
            if not dry_run:
                try:
                    shutil.move(path, dest)
                    moved_count += 1
                except Exception as e:
                    print(f"Error moving {path}: {e}")
    
    print(f"Archival finished. Moved {moved_count} files.")

def run_deletion(dry_run=False):
    print("\n--- Phase 3: Deletion Cleanup ---")
    removed_count = 0
    
    for pattern in DELETE_PATTERNS:
        # Recurse for caches, but avoid .oasis_venv
        search_pattern = os.path.join(SCRATCH_DIR, "**", pattern)
        for path in glob.glob(search_pattern, recursive=True):
            if is_protected(path):
                continue
                
            print(f"{'[DRY RUN] ' if dry_run else ''}Removing: {path}")
            
            if not dry_run:
                try:
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                    else:
                        os.remove(path)
                    removed_count += 1
                except Exception as e:
                    # Likely file in use
                    print(f"Skipping (locked/error): {path}")

    print(f"Deletion finished. Removed {removed_count} items.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="O.A.S.I.S. Phased Workspace Cleanup & Archival")
    parser.add_argument("--archive", action="store_true", help="Run Phase 1: Archival")
    parser.add_argument("--delete", action="store_true", help="Run Phase 3: Deletion (Cleanup)")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without executing")
    
    args = parser.parse_args()
    
    if not args.archive and not args.delete:
        parser.print_help()
        print("\nNote: Use --archive for Phase 1 (Move) or --delete for Phase 3 (Remove). Add --dry-run to preview.")
    else:
        if args.archive:
            run_archival(dry_run=args.dry_run)
        if args.delete:
            if not args.dry_run:
                # Proceed directly since user confirmed in chat
                run_deletion(dry_run=args.dry_run)
            else:
                run_deletion(dry_run=True)
