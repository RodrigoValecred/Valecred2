import pandas as pd

# Mock data based on user description
data = {
    'NOME': ['Grupo A', 'Grupo A', 'Grupo B'],
    'CNPJ': ['111', '222', '333'],
    'Limite': [1000, 1000, 500],
    'Limite Extra': [100, 100, 50],
    'Limite Plus': [10, 10, 5]
}
df = pd.DataFrame(data)
print("Original:")
print(df)

# Sanitize columns (mocking the function)
df.columns = ['nome', 'cnpj', 'limite', 'limite_extra', 'limite_plus']

# Logic to apply
print("\nApplying deduplication logic...")
# We want to group by 'nome' and max the limits
# We drop 'cnpj' because it causes granularity
df_agg = df.groupby('nome', as_index=False).agg({
    'limite': 'max',
    'limite_extra': 'max',
    'limite_plus': 'max'
})

print("\nResult:")
print(df_agg)
