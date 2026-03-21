import sys 
files = ['sql_backend.py', 'main.py'] 
for f in files: 
    data = open(f, 'rb').read().replace(b'\x00', b'') 
    open(f, 'wb').write(data) 
    print(f'Cleaned {f}') 
