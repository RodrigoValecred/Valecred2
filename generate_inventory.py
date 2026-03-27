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

def generate_markdown(assets, inventory):
    md = "# Inventory of Data Assets\n\nThis document provides a detailed inventory of all data assets in the VALECRED project, including Dataflows, Notebooks, Lakehouses, and Warehouses.\n\n"
    for section_name in ['Data Warehouses', 'Lakehouses', 'Dataflows', 'Notebooks']:
        md += f"## {section_name}\n\n"

        section_assets = assets.get(section_name, [])
        for asset in section_assets:
            md += f"### {asset}\n"
            if section_name in inventory and asset in inventory[section_name]:
                for line in inventory[section_name][asset]:
                    md += f"{line}\n"
            else:
                md += "- **Description:** (Missing description)\n"
                if section_name == 'Notebooks':
                    md += "- **Input:** (Not specified)\n"
                    md += "- **Output:** (Not specified)\n"
                    md += "- **Processing Steps:** (Not specified)\n"
                elif section_name == 'Dataflows':
                    md += "- **Source:** (Not specified)\n"
                    md += "- **Destination:** (Not specified)\n"
                    md += "- **Transformations:** (Not specified)\n"
                elif section_name == 'Lakehouses' or section_name == 'Data Warehouses':
                    pass # já possui espaço reservado para descrição
            md += "\n"
    return md

if __name__ == '__main__':
    inventory = parse_inventory('INVENTORY.md')
    assets = find_assets()
    with open('INVENTORY.md', 'w') as f:
        f.write(generate_markdown(assets, inventory))
    print("Generated new inventory in INVENTORY.md")
