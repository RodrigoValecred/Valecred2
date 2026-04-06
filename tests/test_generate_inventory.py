import pytest
from generate_inventory import generate_markdown, parse_inventory

def test_parse_inventory_empty_file(tmp_path):
    f = tmp_path / "inventory.md"
    f.write_text("")
    inventory = parse_inventory(str(f))
    assert inventory == {}


def test_parse_inventory_happy_path(tmp_path):
    f = tmp_path / "inventory.md"
    content = """# Title
Some text

## Notebooks
Some text

### NB_Test.Notebook
- **Description:** A notebook
- **Input:** None

## Dataflows
### DF_Test.Dataflow
- **Description:** A dataflow
"""
    f.write_text(content)
    inventory = parse_inventory(str(f))
    assert inventory == {
        "Notebooks": {
            "NB_Test.Notebook": [
                "- **Description:** A notebook",
                "- **Input:** None"
            ]
        },
        "Dataflows": {
            "DF_Test.Dataflow": [
                "- **Description:** A dataflow"
            ]
        }
    }


def test_parse_inventory_missing_description(tmp_path):
    f = tmp_path / "inventory.md"
    content = """## Notebooks
### NB_Test.Notebook
"""
    f.write_text(content)
    inventory = parse_inventory(str(f))
    assert inventory == {
        "Notebooks": {
            "NB_Test.Notebook": []
        }
    }


def test_parse_inventory_malformed(tmp_path):
    f = tmp_path / "inventory.md"
    content = """### Orphan Asset
- **Description:** description

## Section
- **Description:** orphan description
### Valid Asset
- **Description:** valid
"""
    f.write_text(content)
    inventory = parse_inventory(str(f))
    assert inventory == {
        "Section": {
            "Valid Asset": [
                "- **Description:** valid"
            ]
        }
    }


def test_generate_markdown_empty_inputs():
    assets = {}
    inventory = {}
    md = generate_markdown(assets, inventory)

    # Should contain the header and section titles, but no assets
    assert "# Inventário de Ativos de Dados" in md
    assert "## Data Warehouses" in md
    assert "## Lakehouses" in md
    assert "## Dataflows" in md
    assert "## Notebooks" in md

    # Shouldn't contain any asset markdown like "### "
    assert "### " not in md


def test_generate_markdown_missing_inventory_notebook():
    assets = {
        'Notebooks': ['NB_Test.Notebook']
    }
    inventory = {}
    md = generate_markdown(assets, inventory)

    assert "### NB_Test.Notebook" in md
    assert "- **Description:** (Missing description)" in md
    assert "- **Input:** (Not specified)" in md
    assert "- **Output:** (Not specified)" in md
    assert "- **Processing Steps:** (Not specified)" in md


def test_generate_markdown_missing_inventory_dataflow():
    assets = {
        'Dataflows': ['DF_Test.Dataflow']
    }
    inventory = {}
    md = generate_markdown(assets, inventory)

    assert "### DF_Test.Dataflow" in md
    assert "- **Description:** (Missing description)" in md
    assert "- **Source:** (Not specified)" in md
    assert "- **Destination:** (Not specified)" in md
    assert "- **Transformations:** (Not specified)" in md


def test_generate_markdown_missing_inventory_lakehouse():
    assets = {
        'Lakehouses': ['LH_Test.Lakehouse']
    }
    inventory = {}
    md = generate_markdown(assets, inventory)

    assert "### LH_Test.Lakehouse" in md
    assert "- **Description:** (Missing description)" in md
    # Make sure it doesn't have Notebook/Dataflow specific lines
    assert "- **Input:**" not in md
    assert "- **Source:**" not in md


def test_generate_markdown_existing_inventory():
    assets = {
        'Notebooks': ['NB_Existing.Notebook']
    }
    inventory = {
        'Notebooks': {
            'NB_Existing.Notebook': [
                "- **Description:** This is a test description",
                "- **Input:** Test input",
            ]
        }
    }

    md = generate_markdown(assets, inventory)

    assert "### NB_Existing.Notebook" in md
    assert "- **Description:** This is a test description" in md
    assert "- **Input:** Test input" in md
    # Should not add the missing description placeholders
    assert "(Missing description)" not in md

from unittest.mock import patch

def test_find_assets():
    # Mocking os.walk
    with patch('os.walk') as mock_walk:
        mock_walk.return_value = [
            ('VALECRED_DEV', ['LH_Test.Lakehouse', 'WH_Test.Warehouse'], []),
            ('VALECRED_DEV/subdir', ['DF_Test.Dataflow', 'NB_Test.Notebook', 'NotAnAsset'], []),
        ]

        from generate_inventory import find_assets
        assets = find_assets()

        assert assets == {
            'Lakehouses': ['LH_Test.Lakehouse'],
            'Data Warehouses': ['WH_Test.Warehouse'],
            'Dataflows': ['DF_Test.Dataflow'],
            'Notebooks': ['NB_Test.Notebook'],
        }
