import os

def parse_inventory(filename):
    with open(filename, 'r') as f:
        content = f.read()

    inventory = {}
    current_section = None
    current_item = None

    for line in content.split('\n'):
        if line.startswith('## '):
            current_section = line[3:].strip()
            inventory[current_section] = {}
            current_item = None
        elif line.startswith('### '):
            current_item = line[4:].strip()
            if current_section:
                inventory[current_section][current_item] = []
        elif line.startswith('- ') and current_section and current_item:
            inventory[current_section][current_item].append(line)
    return inventory

inventory = parse_inventory('INVENTORY.md')

def find_assets():
    assets = {
        'Lakehouses': [],
        'Data Warehouses': [],
        'Dataflows': [],
        'Notebooks': [],
    }
    for root, dirs, files in os.walk('VALECRED_DEV'):
        for d in dirs:
            if d.endswith('.Lakehouse'):
                assets['Lakehouses'].append(d)
            elif d.endswith('.Warehouse'):
                assets['Data Warehouses'].append(d)
            elif d.endswith('.Dataflow'):
                assets['Dataflows'].append(d)
            elif d.endswith('.Notebook'):
                assets['Notebooks'].append(d)
    return {k: sorted(v) for k, v in assets.items()}

assets = find_assets()

def generate_markdown(assets, inventory):
    md = "# Inventário de Ativos de Dados\n\nEste documento fornece um inventário detalhado de todos os ativos de dados no projeto VALECRED, incluindo Dataflows, Notebooks, Lakehouses e Warehouses.\n\n"
    for section_name in ['Data Warehouses', 'Lakehouses', 'Dataflows', 'Notebooks']:
        md += f"## {section_name}\n\n"

        section_assets = assets.get(section_name, [])
        for asset in section_assets:
            md += f"### {asset}\n"
            if section_name in inventory and asset in inventory[section_name]:
                for line in inventory[section_name][asset]:
                    md += f"{line}\n"
            else:
                md += "- **Descrição:** (Descrição ausente)\n"
                if section_name == 'Notebooks':
                    md += "- **Entrada:** (Não especificado)\n"
                    md += "- **Saída:** (Não especificado)\n"
                    md += "- **Passos de Processamento:** (Não especificado)\n"
                elif section_name == 'Dataflows':
                    md += "- **Origem:** (Não especificado)\n"
                    md += "- **Destino:** (Não especificado)\n"
                    md += "- **Transformações:** (Não especificado)\n"
                elif section_name == 'Lakehouses' or section_name == 'Data Warehouses':
                    pass # already has description placeholder
            md += "\n"
    return md

with open('INVENTORY.md', 'w') as f:
    f.write(generate_markdown(assets, inventory))
print("Generated new inventory in INVENTORY.md")
