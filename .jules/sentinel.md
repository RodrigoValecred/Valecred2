## 2025-05-15 - [Zip Slip Vulnerability in External Data Loaders]
**Vulnerability:** External data loaders (`NB_Load_Bronze_From_SERPRO.Notebook`, `NB_Load_From_CVM.Notebook`) use `zipfile.ZipFile.extractall()` on ZIP archives downloaded from the internet without validating filenames. This allows a malicious ZIP file to write files outside the target directory (Zip Slip) via path traversal characters (e.g., `../../evil.sh`).
**Learning:** Even trusted government sources can be compromised or spoofed. Standard library functions like `extractall` are not secure by default against malicious archives in older Python versions or without explicit filters.
**Prevention:** Always validate that the destination path of every file in a ZIP archive starts with the intended extraction directory before extracting. Use a `safe_extract` helper function.

## 2026-02-19 - [Missing Timeout in External Requests]
**Vulnerability:** `NB_Load_Bronze_From_BrasilIO.Notebook` performed `requests.get` calls without a `timeout` parameter. This could lead to indefinite hanging of the Spark driver/executor if the external server (Brasil.IO) is unresponsive, causing resource exhaustion (DoS).
**Learning:** Default behavior of Python's `requests` library is to wait forever. This is dangerous in production data pipelines.
**Prevention:** Enforce a `timeout` (e.g., 60s) on all external network calls. Added a static analysis test `tests/test_brasil_io_security.py` to enforce this pattern in the future.

## 2026-02-19 - [Memory Exhaustion in Large File Downloads]
**Vulnerability:** `NB_Load_From_CVM.Notebook` downloaded large ZIP files (potential >500MB) directly into memory using `requests.get().content`, causing potential OOM (Out of Memory) errors and driver failures on constrained Spark clusters.
**Learning:** Default `requests.get` behavior loads the entire response body into RAM. For data engineering pipelines handling external datasets, this is a critical scalability and availability risk.
**Prevention:** Always use `stream=True` and `iter_content(chunk_size=...)` for downloading files, regardless of the expected file size.

## 2026-02-23 - [Hardcoded Credentials in RealTime Notebook]
**Vulnerability:** `VALECRED_DEV/8_RealTime/KPI_DA_TV.Notebook` contained a hardcoded password string (`.option("password", "sua_senha")`).
**Learning:** Even placeholder passwords in committed code can be dangerous if they are accidentally deployed or if real credentials are later substituted and committed. Hardcoded secrets are a top security risk.
**Prevention:** Always use a secrets management service (like Azure Key Vault) via `mssparkutils.credentials.getSecret` instead of string literals. Added `tests/test_kpi_security.py` to strictly enforce this.

## 2026-02-19 - [Insecure HTTP Request Validation]
**Vulnerability:** `NB_Load_Bronze_Receita_Federal_Full.Notebook` performed `requests.get` and `requests.head` calls with `verify=False` and used `urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)`. This disables SSL verification, leaving the application vulnerable to Man-in-the-Middle (MitM) attacks where bad actors could intercept the data transfer.
**Learning:** Downloading remote data without verifying certificates poses a risk, particularly when the data may impact sensitive internal workflows. Using `verify=False` should be avoided in production.
**Prevention:** Ensure that external requests are made securely with `verify=True` and avoid suppressing the insecure request warnings.
