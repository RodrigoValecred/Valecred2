import json
import os
import re

notebook_names = [
    "NB_Analyze_FIDC_Performance",
    "NB_Gold_Risco_Cliente",
    "NB_Gold_Relatorio_Novos_Clientes",
    "NB_Analise_Safra_Gerentes",
    "NB_Inadimplencia_Mensal",
    "NB_Risk_Aggregation",
    "NB_Gold_Carteira_Titulos",
    "NB_Analise_Cliente_Especifico",
    "NB_Calendario_Gold",
    "NB_Gold_Relatorio_Produtos_Mensal",
    "NB_Gold_Relatorio_Limites_Especificos",
    "NB_Fechamento_Prorrogacao_Mensal",
    "NB_Gold_Dim_Produtos"
]

files = os.popen('find VALECRED_DEV/5_Notebooks -name "notebook-content.py"').read().splitlines()

notebook_paths = {}
for name in notebook_names:
    for f in files:
        if name in f:
            notebook_paths[name] = f
            break

deps_map = {}

for name, path in notebook_paths.items():
    with open(path, 'r') as file:
        content = file.read()
        reads = set(re.findall(r'(?:spark\.read\.table|spark\.table|spark\.sql)\(["\']([^"\']+)["\']\)', content))
        reads.update(re.findall(r'FROM\s+([A-Za-z0-9_\.]+)', content))

        writes = set()

        lines = content.split('\n')
        for i, line in enumerate(lines):
            if 'saveAsTable' in line or 'write.format("delta").save' in line or 'write.mode("overwrite").saveAsTable' in line:
                match = re.search(r'saveAsTable\s*\(\s*["\']([^"\']+)["\']\s*\)', line)
                if match:
                    writes.add(match.group(1))
                else:
                    match = re.search(r'save\s*\(\s*["\']([^"\']+)["\']\s*\)', line)
                    if match:
                         writes.add(match.group(1))
                    else:
                        for j in range(max(0, i-5), i+1):
                            if 'output_table' in lines[j]:
                                m = re.search(r'output_table\s*=\s*["\']([^"\']+)["\']', lines[j])
                                if m: writes.add(m.group(1))
                            if 'target_table' in lines[j]:
                                m = re.search(r'target_table\s*=\s*["\']([^"\']+)["\']', lines[j])
                                if m: writes.add(m.group(1))

        if not writes:
             for line in lines:
                 if 'target_table' in line or 'output_table' in line or 'table_name' in line:
                     m = re.search(r'(?:target_table|output_table|table_name)\s*=\s*["\']([^"\']+)["\']', line)
                     if m: writes.add(m.group(1))

        deps_map[name] = {"reads": list(reads), "writes": list(writes)}

# Corrige algumas escritas que podem ter sido perdidas com base no nome do arquivo ou contexto
deps_map['NB_Gold_Dim_Produtos']['writes'] = ['LH_Gold.dim_produtos']
deps_map['NB_Calendario_Gold']['writes'] = ['LH_Gold.dim_calendario']

dependencies = {}
for nb, info in deps_map.items():
    dependencies[nb] = []
    for read_table in info['reads']:
        for other_nb, other_info in deps_map.items():
            if nb != other_nb and read_table in other_info['writes']:
                dependencies[nb].append(other_nb)

# Desduplica
for nb in dependencies:
    dependencies[nb] = list(set(dependencies[nb]))

import pprint
print("Discovered dependencies:")
pprint.pprint(dependencies)

pipeline_path = "VALECRED_DEV/2_Pipelines/PL_Relatorios_Gold_Diaria.DataPipeline/pipeline-content.json"
with open(pipeline_path, 'r') as f:
    data = json.load(f)

for activity in data['properties']['activities']:
    name = activity['name']
    if name in dependencies and dependencies[name]:
        activity['dependsOn'] = []
        for dep in dependencies[name]:
            activity['dependsOn'].append({
                "activity": dep,
                "dependencyConditions": ["Succeeded"]
            })
    else:
        activity['dependsOn'] = []

with open(pipeline_path, 'w') as f:
    json.dump(data, f, indent=2)
