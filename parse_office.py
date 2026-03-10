import json
import glob

for f in glob.glob("VALECRED_DEV/2_Pipelines/**/*.json", recursive=True):
    with open(f, "r") as file:
        data = json.load(file)
        if "properties" in data and "activities" in data["properties"]:
            for a in data["properties"]["activities"]:
                if "Office365" in a["type"]:
                    print(f, a["name"], a["type"], a["externalReferences"])
