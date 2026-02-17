import os
import zipfile
import shutil
import unittest
import tempfile

# --- The function to be tested and later copied to notebook ---
def safe_extract(zip_ref, path):
    """
    Extracts a zip file to the specified path, preventing Zip Slip vulnerability.
    """
    # Normalize the target path to an absolute path
    target_path = os.path.abspath(path)

    for member in zip_ref.namelist():
        # Resolve the full path of the member
        # Note: os.path.join will discard 'target_path' if 'member' is absolute
        member_path = os.path.join(target_path, member)
        # Normalize the member path to resolve '..' and '.'
        abs_member_path = os.path.abspath(member_path)

        # Check if the member path starts with the target path
        # We append os.sep to ensure we match directory boundaries (e.g. /tmp/foo vs /tmp/foobar)
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
        """Test extracting a zip file with Zip Slip vulnerability."""

        class MockZipFile:
            def __init__(self, namelist_return):
                self._namelist = namelist_return

            def namelist(self):
                return self._namelist

            def extractall(self, path):
                pass # Mock extraction

        # Case 1: Simple parent traversal
        mock_zip = MockZipFile(['../evil.txt'])
        with self.assertRaisesRegex(Exception, "Zip Slip vulnerability detected"):
            safe_extract(mock_zip, self.extract_path)

        # Case 2: Nested traversal
        mock_zip = MockZipFile(['folder/../../evil.txt'])
        with self.assertRaisesRegex(Exception, "Zip Slip vulnerability detected"):
            safe_extract(mock_zip, self.extract_path)

        # Case 3: Absolute path
        mock_zip_abs = MockZipFile(['/tmp/evil.txt'])
        with self.assertRaisesRegex(Exception, "Zip Slip vulnerability detected"):
            safe_extract(mock_zip_abs, self.extract_path)

if __name__ == '__main__':
    unittest.main()
