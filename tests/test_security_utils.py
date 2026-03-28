import os
import zipfile
import shutil
import unittest
import tempfile

# --- A função a ser testada e posteriormente copiada para o notebook ---
def safe_extract(zip_ref, path):
    """
    Extrai um arquivo zip para o caminho especificado, prevenindo a vulnerabilidade Zip Slip.
    """
    # Normaliza o caminho de destino para um caminho absoluto
    target_path = os.path.abspath(path)

    for member in zip_ref.namelist():
        # Resolve o caminho completo do membro
        # Nota: os.path.join descartará 'target_path' se 'member' for absoluto
        member_path = os.path.join(target_path, member)
        # Normaliza o caminho do membro para resolver '..' e '.'
        abs_member_path = os.path.abspath(member_path)

        # Verifica se o caminho do membro inicia com o caminho de destino
        # Nós anexamos os.sep para garantir a correspondência de limites de diretório (ex. /tmp/foo vs /tmp/foobar)
        if not abs_member_path.startswith(os.path.join(target_path, '')) and not abs_member_path == target_path:
             raise Exception(f"Zip Slip vulnerability detected: {member}")

    zip_ref.extractall(path)

class TestSafeExtract(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.zip_path = os.path.join(self.temp_dir, "test.zip")
        self.extract_path = os.path.join(self.temp_dir, "extracted")
        os.makedirs(self.extract_path)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_safe_extract_normal(self):
        """Test extracting a normal zip file."""
        with zipfile.ZipFile(self.zip_path, 'w') as zf:
            zf.writestr('test.txt', 'This is a test file.')
            zf.writestr('folder/nested.txt', 'Nested file.')

        with zipfile.ZipFile(self.zip_path, 'r') as zf:
            safe_extract(zf, self.extract_path)

        self.assertTrue(os.path.exists(os.path.join(self.extract_path, 'test.txt')))
        self.assertTrue(os.path.exists(os.path.join(self.extract_path, 'folder', 'nested.txt')))

    def test_safe_extract_zip_slip(self):
        """Testa a extração de um arquivo zip com a vulnerabilidade Zip Slip."""

        class MockZipFile:
            def __init__(self, namelist_return):
                self._namelist = namelist_return

            def namelist(self):
                return self._namelist

            def extractall(self, path):
                pass # Simulação da extração

        # Caso 1: Travessia simples de diretório pai
        mock_zip = MockZipFile(['../evil.txt'])
        with self.assertRaisesRegex(Exception, "Zip Slip vulnerability detected"):
            safe_extract(mock_zip, self.extract_path)

        # Caso 2: Travessia aninhada
        mock_zip = MockZipFile(['folder/../../evil.txt'])
        with self.assertRaisesRegex(Exception, "Zip Slip vulnerability detected"):
            safe_extract(mock_zip, self.extract_path)

        # Caso 3: Caminho absoluto
        mock_zip_abs = MockZipFile(['/tmp/evil.txt'])
        with self.assertRaisesRegex(Exception, "Zip Slip vulnerability detected"):
            safe_extract(mock_zip_abs, self.extract_path)

if __name__ == '__main__':
    unittest.main()
