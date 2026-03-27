import pytest
from generate_inventory import generate_markdown

def test_generate_markdown_empty_inputs():
    assets = {}
    inventory = {}
    md = generate_markdown(assets, inventory)

    # Should contain the header and section titles, but no assets
    assert "# Inventory of Data Assets" in md
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
