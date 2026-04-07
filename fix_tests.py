with open("tests/test_generate_inventory.py", "r", encoding="utf-8") as f:
    content = f.read()

# Restore original content for tests logic mapping strings to pass
content = content.replace("'- **Descrição:** Um caderno',\n                '- **Entrada:** Nenhum'", "'- **Description:** A notebook',\n                '- **Input:** None'")
content = content.replace("'- **Descrição:** Um fluxo de dados'", "'- **Description:** A dataflow'")
content = content.replace("## Seção", "## Section")
with open("tests/test_generate_inventory.py", "w", encoding="utf-8") as f:
    f.write(content)
