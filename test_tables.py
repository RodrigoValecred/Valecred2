import json
import os
import re

files = os.popen('find VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold -name "notebook-content.py"').read().splitlines()

for path in files:
    name = path.split('/')[-2].replace('.Notebook', '')
    with open(path, 'r') as file:
        content = file.read()
        reads = set(re.findall(r'(?:spark\.read\.table|spark\.table)\(["\']([^"\']+)["\']\)', content))
        writes = set()
        for line in content.split('\n'):
            if 'saveAsTable' in line or 'write.format("delta").save' in line or 'write.mode("overwrite").saveAsTable' in line:
                match = re.search(r'saveAsTable\s*\(\s*["\']([^"\']+)["\']\s*\)', line)
                if match:
                    writes.add(match.group(1))

        # Verificação estática
        if not writes:
             for line in content.split('\n'):
                 if 'target_table' in line or 'output_table' in line or 'table_name' in line:
                     m = re.search(r'(?:target_table|output_table|table_name)\s*=\s*["\']([^"\']+)["\']', line)
                     if m: writes.add(m.group(1))

        print(f"{name}: reads={list(reads)} writes={list(writes)}")
