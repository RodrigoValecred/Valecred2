import json
import glob

for f in glob.glob("VALECRED_DEV/2_Pipelines/**/*.json", recursive=True):
    with open(f, "r") as file:
        content = file.read()
        if "Office365" in content or "Office 365" in content or "Outlook" in content or "email" in content or "Email" in content:
            print(f)
