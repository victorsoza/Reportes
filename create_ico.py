import os
import sys
from PIL import Image

root = os.path.dirname(__file__)
src = os.path.join(root, 'Iconos', 'ComprasInternacionales.png')
dst = os.path.join(root, 'Iconos', 'ComprasInternacionales.ico')

if not os.path.exists(src):
    print(f"ERROR: archivo no encontrado: {src}")
    sys.exit(2)

try:
    img = Image.open(src).convert('RGBA')
    sizes = [(256,256),(128,128),(64,64),(48,48),(32,32),(16,16)]
    img.save(dst, format='ICO', sizes=sizes)
    print('OK: creado', dst)
except Exception as e:
    print('ERROR al crear ICO:', e)
    sys.exit(1)
