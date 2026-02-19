## 2025-05-15 - [Zip Slip Vulnerability in External Data Loaders]
**Vulnerability:** External data loaders (`NB_Load_Bronze_From_SERPRO.Notebook`, `NB_Load_From_CVM.Notebook`) use `zipfile.ZipFile.extractall()` on ZIP archives downloaded from the internet without validating filenames. This allows a malicious ZIP file to write files outside the target directory (Zip Slip) via path traversal characters (e.g., `../../evil.sh`).
**Learning:** Even trusted government sources can be compromised or spoofed. Standard library functions like `extractall` are not secure by default against malicious archives in older Python versions or without explicit filters.
**Prevention:** Always validate that the destination path of every file in a ZIP archive starts with the intended extraction directory before extracting. Use a `safe_extract` helper function.

## 2026-02-19 - [Missing Timeout in External Requests]
**Vulnerability:** `NB_Load_Bronze_From_BrasilIO.Notebook` performed `requests.get` calls without a `timeout` parameter. This could lead to indefinite hanging of the Spark driver/executor if the external server (Brasil.IO) is unresponsive, causing resource exhaustion (DoS).
**Learning:** Default behavior of Python's `requests` library is to wait forever. This is dangerous in production data pipelines.
**Prevention:** Enforce a `timeout` (e.g., 60s) on all external network calls. Added a static analysis test `tests/test_brasil_io_security.py` to enforce this pattern in the future.
