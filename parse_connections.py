import json

with open("VALECRED_DEV/2_Pipelines/PL_Treinamento_Semanal_da_VAI.DataPipeline/pipeline-content.json", "r") as f:
    data = json.load(f)

for a in data["properties"]["activities"]:
    if "externalReferences" in a:
        print(a["name"], a["externalReferences"])
