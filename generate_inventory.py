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

def find_assets():
    suffix_map = {
        '.Lakehouse': 'Lakehouses',
        '.Warehouse': 'Data Warehouses',
        '.Dataflow': 'Dataflows',
        '.Notebook': 'Notebooks',
    }
    assets = {v: [] for v in suffix_map.values()}

    for root, dirs, files in os.walk('VALECRED_DEV'):
        for d in dirs:
            for suffix, asset_type in suffix_map.items():
                if d.endswith(suffix):
                    assets[asset_type].append(d)
                    break
    return {k: sorted(v) for k, v in assets.items()}

def generate_markdown(assets, inventory):
    md = []
    md.append("# Inventário de Ativos de Dados\n\nEste documento fornece um inventário detalhado de todos os ativos de dados no projeto VALECRED, incluindo Dataflows, Notebooks, Lakehouses e Warehouses.\n\n")
    for section_name in ['Data Warehouses', 'Lakehouses', 'Dataflows', 'Notebooks']:
        md.append(f"## {section_name}\n\n")

        section_assets = assets.get(section_name, [])
        for asset in section_assets:
            md.append(f"### {asset}\n")
            if section_name in inventory and asset in inventory[section_name]:
                for line in inventory[section_name][asset]:
                    md.append(f"{line}\n")
            else:
                md.append("- **Description:** (Missing description)\n")
                if section_name == 'Notebooks':
                    md.append("- **Input:** (Not specified)\n")
                    md.append("- **Output:** (Not specified)\n")
                    md.append("- **Processing Steps:** (Not specified)\n")
                elif section_name == 'Dataflows':
                    md.append("- **Source:** (Not specified)\n")
                    md.append("- **Destination:** (Not specified)\n")
                    md.append("- **Transformations:** (Not specified)\n")
                elif section_name == 'Lakehouses' or section_name == 'Data Warehouses':
                    pass # já possui espaço reservado para descrição
            md.append("\n")
    return "".join(md)

if __name__ == '__main__':
    inventory = parse_inventory('INVENTORY.md')
    assets = find_assets()
    with open('INVENTORY.md', 'w') as f:
        f.write(generate_markdown(assets, inventory))
    print("Generated new inventory in INVENTORY.md")
