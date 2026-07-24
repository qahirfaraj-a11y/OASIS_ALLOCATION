@echo off
echo Compiling Release...
python setup_cython.py build_ext --inplace
pause
