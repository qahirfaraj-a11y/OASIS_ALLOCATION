import os
import glob

print('Files in C:/Oasis:')
for f in glob.glob('C:/Oasis/**/*', recursive=True):
    if os.path.isfile(f):
        print(f)
