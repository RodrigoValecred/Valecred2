with open("tests/test_generate_inventory.py", "r", encoding="utf-8") as f:
    content = f.read()

# Restaurar o conteúdo original para a lógica dos testes de mapeamento de strings passarem
content = content.replace("'- **Descrição:** Um caderno',\n                '- **Entrada:** Nenhum'", "'- **Description:** A notebook',\n                '- **Input:** None'")
content = content.replace("'- **Descrição:** Um fluxo de dados'", "'- **Description:** A dataflow'")
content = content.replace("## Seção", "## Section")
with open("tests/test_generate_inventory.py", "w", encoding="utf-8") as f:
    f.write(content)
