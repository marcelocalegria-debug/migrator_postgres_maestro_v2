import os
import re

FILES_TO_FIX = [
    'PosMigracao_comparaChecksum_bytea.py',
    'repair_fk_scripts.py',
    'migrator_v2.py',
    'migrator_smalltables_v2.py',
    'migrator_parallel_doc_oper_v2.py',
    'migrator_log_eventos_v2.py',
    'compara_estrutura_fb2pg.py',
    'compara_cont_fb2pg.py'
]

NEW_DLL_LOGIC = """if os.name == "nt":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    _fb_paths = [
        os.path.join(script_dir, "fbclient.dll"),
        os.path.abspath("fbclient.dll"),
        r"C:\\Program Files\\Firebird\\Firebird_3_0\\fbclient.dll",
        r"C:\\Program Files\\Firebird\\Firebird_4_0\\fbclient.dll",
        r"C:\\Program Files\\Firebird\\Firebird_5_0\\fbclient.dll",
        r"C:\\Program Files\\Firebird\\Firebird_2_5\\bin\\fbclient.dll",
        r"C:\\Program Files (x86)\\Firebird\\Firebird_3_0\\fbclient.dll",
        r"C:\\Program Files (x86)\\Firebird\\Firebird_2_5\\bin\\fbclient.dll",
    ]
    for _p in _fb_paths:
        if os.path.exists(_p):
            try:
                fdb.load_api(_p)
                break
            except Exception:
                pass
"""

for file_path in FILES_TO_FIX:
    if not os.path.exists(file_path):
        print(f"File {file_path} not found, skipping.")
        continue
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    pattern = re.compile(r'if os\.name == [\'"]nt[\'"]:.+?fdb\.load_api\(.+?\)', re.DOTALL)
    
    match = pattern.search(content)
    if match:
        print(f"Fixing {file_path}...")
        old_text = match.group(0)
        new_content = content.replace(old_text, NEW_DLL_LOGIC)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
    else:
        print(f"Could not find DLL loading pattern in {file_path}")
