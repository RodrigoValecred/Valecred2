file_path = "VALECRED_DEV/1_Dataflows/Dataflows_Silver/DF_Preparacao_Silver.Dataflow/mashup.pq"

with open(file_path, "r") as f:
    content = f.read()

# Lines to modify
content = content.replace('"TDOC", "FLOATING", "PERCCUSTOFINANC"', '"TDOC", "PERCCUSTOFINANC"')
content = content.replace('"DTLIMITEDESCONTO", "FLOATING", "VALORSISCOB"', '"DTLIMITEDESCONTO", "VALORSISCOB"')

with open(file_path, "w") as f:
    f.write(content)

print("Updated mashup.pq")
