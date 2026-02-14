
import unittest
import requests
import hashlib

class TestRFBVerify(unittest.TestCase):
    def test_verify_rfb_hashes(self):
        BASE_URL = "https://raw.githubusercontent.com/turicas/socios-brasil/7b56360e93f35349fe29588dddf7d3c8b07eb22b/"

        expected_hashes = {
            "extract_dump.py": "c53801ddd2e4c04fd69c5d7179b48e365b50048bd6840d27f0d4be7ab0b8e4f4",
            "requirements.txt": "ef9f18112ebaf55988be6cdc869e3382ed224d0ca9cefe382f001f1659431f3f"
        }

        for filename, expected_hash in expected_hashes.items():
            url = f"{BASE_URL}{filename}"
            print(f"Verifying {url}...")

            response = requests.get(url)
            self.assertEqual(response.status_code, 200, f"Failed to download {filename}")

            file_hash = hashlib.sha256(response.content).hexdigest()
            print(f"Calculated hash: {file_hash}")
            print(f"Expected hash: {expected_hash}")

            self.assertEqual(file_hash, expected_hash, f"Hash mismatch for {filename}")

if __name__ == '__main__':
    unittest.main()
