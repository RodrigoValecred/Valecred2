import json

with open("VALECRED_DEV/2_Pipelines/PL_Orquestracao_de_Dados_Incremental.DataPipeline/pipeline-content.json", "r") as f:
    data = json.load(f)

activities = data["properties"]["activities"]
activity_names = [a["name"] for a in activities]
print("Activities:")
print(activity_names)

# Find activities that no one depends on
has_dependents = set()
for a in activities:
    for dep in a.get("dependsOn", []):
        has_dependents.add(dep["activity"])

leaf_activities = [name for name in activity_names if name not in has_dependents]
print("\nLeaf activities (no one depends on them):")
print(leaf_activities)
