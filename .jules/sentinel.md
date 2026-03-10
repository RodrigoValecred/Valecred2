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

## 2026-03-08 - [Missing SSL Verification in External Requests]
**Vulnerability:** External network request to CVM (`requests.head` and `requests.get`) in `NB_Load_From_CVM.Notebook` did not strictly enforce SSL certificate verification (`verify=True` was missing). This can make the application vulnerable to Man-in-the-Middle (MitM) attacks where an attacker intercepts or tampers with the connection.
**Learning:** Relying on default configuration of requests can sometimes expose pipelines if SSL is not explicitly enforced, particularly when downloading critical financial datasets from external sources.
**Prevention:** Always explicitly include `verify=True` when calling `requests.get()` or `requests.head()`.

## 2026-03-08 - [Insecure HTTP Fallback Mirror]
**Vulnerability:** The RFB data loader `NB_Load_Bronze_Receita_Federal_Full.Notebook` used an unencrypted IP address (`http://200.152.38.155/CNPJ/`) as a fallback mirror in the `MIRRORS` list. If the primary site failed, the pipeline silently fell back to an insecure HTTP connection, exposing the download to Man-in-the-Middle (MitM) attacks.
**Learning:** High availability workarounds (like fallback mirrors or direct IP addresses) often bypass standard security controls (like HTTPS/SSL) out of convenience or lack of infrastructure, silently introducing vulnerabilities when the primary system goes down.
**Prevention:** Ensure that all fallback infrastructure, mirrors, and alternative endpoints adhere to the same security standards (HTTPS/TLS) as the primary endpoint. Remove insecure mirrors and replace them with secure alternatives (e.g., GitHub Releases).

## 2026-03-09 - Prevent MitM in RFB Downloads
**Vulnerability:** The data extraction notebook `NB_Extract_Bronze_Receita_Federal_Full.Notebook` used an insecure, plaintext HTTP IP address (`http://200.152.38.155/CNPJ/`) as a fallback mirror for Receita Federal downloads. This exposed the pipeline to Man-in-the-Middle (MitM) attacks where malicious payloads could be intercepted or injected.
**Learning:** Hardcoded IP fallbacks in external data ingestion pipelines are often added for reliability but introduce severe security risks if they don't enforce TLS/HTTPS.
**Prevention:** Always enforce HTTPS for external data downloads. Remove plaintext HTTP fallbacks entirely if a secure alternative exists.
## 2026-03-08 - [Insecure HTTP Fallback Mirror]
**Vulnerability:** The RFB data loader `NB_Extract_Bronze_Receita_Federal_Full.Notebook` used an unencrypted IP address (`http://200.152.38.155/CNPJ/`) as a fallback mirror in the `MIRRORS` list. If the primary site failed, the pipeline silently fell back to an insecure HTTP connection, exposing the download to Man-in-the-Middle (MitM) attacks.
**Learning:** High availability workarounds (like fallback mirrors or direct IP addresses) often bypass standard security controls (like HTTPS/SSL) out of convenience or lack of infrastructure, silently introducing vulnerabilities when the primary system goes down.
**Prevention:** Ensure that all fallback infrastructure, mirrors, and alternative endpoints adhere to the same security standards (HTTPS/TLS) as the primary endpoint. Remove insecure mirrors and replace them with secure alternatives (e.g., GitHub Releases).
## 2026-03-10 - [Missing Timeout and SSL Verification in RFB URL Test]
**Vulnerability:** `tests/test_verify_rfb_url.py` performed a `requests.get` call without a `timeout` and `verify=True` parameter. This could lead to indefinite hanging if the external server (GitHub) is unresponsive, and exposes the connection to Man-in-the-Middle (MitM) attacks.
**Learning:** Default behavior of Python's `requests` library is to wait forever and sometimes SSL verification is forgotten. This is dangerous in CI/CD environments as it can freeze test pipelines.
**Prevention:** Enforce a `timeout` (e.g., 30s) and `verify=True` on all external network calls, even in tests.
