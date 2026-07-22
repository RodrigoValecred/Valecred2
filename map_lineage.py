import os
import re

def parse_notebook_files(directory):
    writes = {}
    reads = {}

    # regex patterns
    save_pattern = re.compile(r'\.saveAsTable\s*\(\s*f?["\']([^"\']+)["\']\s*\)', re.IGNORECASE)
    read_pattern = re.compile(r'spark\.read\.table\s*\(\s*f?["\']([^"\']+)["\']\s*\)', re.IGNORECASE)
    table_pattern = re.compile(r'spark\.table\s*\(\s*f?["\']([^"\']+)["\']\s*\)', re.IGNORECASE)

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py') or file.endswith('.sql'):
                filepath = os.path.join(root, file)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = f.read()

                        # Find writes
                        for match in save_pattern.finditer(content):
                            table = match.group(1).replace('{target_lakehouse}', 'LH_Silver').replace('{source_lakehouse}', 'LH_Bronze')
                            if table not in writes:
                                writes[table] = []
                            writes[table].append(filepath)

                        # Find reads (read.table)
                        for match in read_pattern.finditer(content):
                            table = match.group(1).replace('{target_lakehouse}', 'LH_Silver').replace('{source_lakehouse}', 'LH_Bronze')
                            if table not in reads:
                                reads[table] = []
                            reads[table].append(filepath)

                        # Find reads (spark.table)
                        for match in table_pattern.finditer(content):
                            table = match.group(1).replace('{target_lakehouse}', 'LH_Silver').replace('{source_lakehouse}', 'LH_Bronze')
                            if table not in reads:
                                reads[table] = []
                            reads[table].append(filepath)

                except Exception as e:
                    print(f"Error reading {filepath}: {e}")

    return writes, reads

writes, reads = parse_notebook_files('VALECRED_DEV')

print("--- TABLES WRITTEN BUT NOT READ (Potential Loose Ends) ---")
for t in sorted(writes.keys()):
    # We might miss dynamic reads, but static ones are caught
    if t not in reads and 'teste' not in t.lower() and 'tmp' not in t.lower() and t != 'LH_Bronze.inventario_completo_detalhado' and t != 'LH_Bronze.relatorio_frequencia_acessos':
        print(f"Table: {t}")
        for w in writes[t]:
            print(f"  Written in: {w}")

print("\n--- TABLES READ BUT NOT WRITTEN IN NOTEBOOKS (Source or Missing) ---")
for t in sorted(reads.keys()):
    if t not in writes and 'LH_Bronze' not in t:
        print(f"Table: {t}")
        for r in reads[t]:
            print(f"  Read in: {r}")

print("\n--- MULTIPLE WRITERS (Potential conflict/duplication) ---")
for t in sorted(writes.keys()):
    unique_writers = set(writes[t])
    if len(unique_writers) > 1:
        print(f"Table: {t} has {len(unique_writers)} writers:")
        for w in unique_writers:
            print(f"  - {w}")
