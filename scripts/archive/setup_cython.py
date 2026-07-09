"""
O.A.S.I.S. Cython Build Configuration
=======================================
Compiles the core intellectual property (oasis/logic/*.py) into
native Windows C-extensions (.pyd) to prevent reverse engineering.

Usage:
    python setup_cython.py build_ext --inplace
"""

import os
from setuptools import setup, Extension
from Cython.Build import cythonize

# Define the target directory containing the IP we want to protect
TARGET_DIR = os.path.join("oasis", "logic")

# Find all .py files in the target directory (exclude __init__.py)
python_files = []
for root, dirs, files in os.walk(TARGET_DIR):
    for file in files:
        if file.endswith(".py") and file != "__init__.py":
            # We want the relative path from the directory where setup.py is run
            filepath = os.path.relpath(os.path.join(root, file))
            python_files.append(filepath)

if not python_files:
    print(f"No Python files found in {TARGET_DIR} to compile.")
    exit(0)

# Create an Extension object for each file
extensions = []
for py_file in python_files:
    # Convert path 'oasis\logic\module.py' to module name 'oasis.logic.module'
    module_name = os.path.splitext(py_file)[0].replace(os.path.sep, ".")
    
    ext = Extension(
        name=module_name,
        sources=[py_file],
        # Add any required C compiler flags here if needed
        # extra_compile_args=["/O2"] # Windows optimization flag
    )
    extensions.append(ext)

# Run setup
setup(
    name="OASIS_Protected_Core",
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            'language_level': "3",
            'always_allow_keywords': True,
        },
        build_dir="build",      # Temp directory for .c files
        annotate=False,         # Don't generate HTML annotation files
    ),
    zip_safe=False,
)
