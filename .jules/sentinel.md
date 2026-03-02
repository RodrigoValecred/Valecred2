## 2026-03-02 - Enforce SSL Verification for Data Downloads
**Vulnerability:** A critical security vulnerability (CWE-295) was found in `NB_Load_Bronze_Receita_Federal_Full.Notebook` where external HTTPS requests to download Receita Federal data disabled SSL validation (`verify=False`) and suppressed the resulting warnings.
**Learning:** Hardcoding `verify=False` for convenience bypasses fundamental security checks, leaving the data pipeline exposed to Man-in-the-Middle (MitM) attacks where external data could be intercepted or manipulated before entering the Data Lakehouse.
**Prevention:** Always enforce `verify=True` for all HTTP requests to external sources. Address certificate errors by providing the correct CA bundles rather than disabling validation entirely.
