import unittest
import os

class TestSecurityFixes(unittest.TestCase):

    def test_requirements_file_exists(self):
        """Verify that requirements.txt exists."""
        self.assertTrue(os.path.exists("requirements.txt"), "requirements.txt should exist")

    def test_requirements_content(self):
        """Verify that requirements.txt contains the necessary packages."""
        if not os.path.exists("requirements.txt"):
            self.skipTest("requirements.txt not found")

        with open("requirements.txt", "r") as f:
            content = f.read()

        required_packages = ["fastapi", "uvicorn", "requests", "httpx", "typing_extensions>=4.10.0"]
        for package in required_packages:
            self.assertIn(package, content, f"{package} should be in requirements.txt")

    def test_notebook_no_insecure_install(self):
        """Verify that the notebook does not contain insecure pip install calls."""
        notebook_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Utilitarios/NB_CERC_Consulta_API.Notebook/notebook-content.py"
        if not os.path.exists(notebook_path):
            self.skipTest(f"Notebook not found at {notebook_path}")

        with open(notebook_path, "r") as f:
            content = f.read()

        # Check for the specific insecure pattern we removed
        self.assertNotIn('subprocess.check_call([sys.executable, "-m", "pip", "install", package])', content, "Insecure pip install pattern found in notebook")
        self.assertNotIn('install("fastapi")', content, "Insecure install('fastapi') found in notebook")

if __name__ == '__main__':
    unittest.main()
