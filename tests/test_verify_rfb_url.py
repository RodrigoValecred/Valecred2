import re
import os

def test_rfb_url_pinned():
    # Path relative to repo root
    notebook_path = "VALECRED_DEV/7_Dados_Externos/NB_Load_Bronze_Receita_Federal_Full.Notebook/notebook-content.py"

    # Check if file exists (handle running from root or tests/)
    if not os.path.exists(notebook_path):
        # Try adjusting path if running from tests/
        notebook_path = os.path.join("..", notebook_path)

    # If still not found, try absolute path based on cwd
    if not os.path.exists(notebook_path):
         notebook_path = os.path.join(os.getcwd(), "VALECRED_DEV/7_Dados_Externos/NB_Load_Bronze_Receita_Federal_Full.Notebook/notebook-content.py")

    assert os.path.exists(notebook_path), f"Notebook file not found at {notebook_path}"

    with open(notebook_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Look for base_repo_url assignment
    # We expect: base_repo_url = "https://raw.githubusercontent.com/turicas/socios-brasil/<COMMIT_HASH>/"
    match = re.search(r'base_repo_url\s*=\s*"(https://raw\.githubusercontent\.com/turicas/socios-brasil/([^/]+)/)"', content)

    assert match, "base_repo_url variable not found or format changed in notebook-content.py"

    version_ref = match.group(2)

    # Check if it is a commit hash (40 hex chars)
    # This prevents using 'master', 'main', or other branch names which are mutable
    is_commit_hash = re.match(r"^[a-f0-9]{40}$", version_ref)

    assert is_commit_hash, f"Security Risk: URL is not pinned to a specific commit hash. Found version reference: '{version_ref}'. Please pin to a full SHA-1 hash (e.g., 7b56360e93f35349fe29588dddf7d3c8b07eb22b)."

def test_rfb_security_checks():
    # Path relative to repo root
    notebook_path = "VALECRED_DEV/7_Dados_Externos/NB_Load_Bronze_Receita_Federal_Full.Notebook/notebook-content.py"

    if not os.path.exists(notebook_path):
        notebook_path = os.path.join("..", notebook_path)

    if not os.path.exists(notebook_path):
         notebook_path = os.path.join(os.getcwd(), "VALECRED_DEV/7_Dados_Externos/NB_Load_Bronze_Receita_Federal_Full.Notebook/notebook-content.py")

    assert os.path.exists(notebook_path)

    with open(notebook_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. Check for hashlib import
    assert "import hashlib" in content, "Security Risk: 'hashlib' not imported in notebook-content.py"

    # 2. Check for SHA256 verification logic
    assert "hashlib.sha256(r.content).hexdigest()" in content, "Security Risk: SHA256 hash calculation missing in notebook-content.py"
    assert "expected_hashes" in content, "Security Risk: 'expected_hashes' dictionary missing in notebook-content.py"

    # 3. Check for timeout in requests.get
    assert "timeout=60" in content, "Security Risk: 'timeout=60' missing in requests.get call in notebook-content.py"
