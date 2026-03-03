import time
import pandas as pd
import numpy as np

num_rows = 1000000
data = {f'feature_{i}': np.random.rand(num_rows) * 10000 for i in range(20)}
data['CODSTATUSCLIENTE'] = np.random.choice(['A', 'B', 'C'], size=num_rows)
data['CODRATING_CEDENTE'] = np.random.choice(['AA', 'A', 'B', 'C'], size=num_rows)

df_pd = pd.DataFrame(data)

# Baseline approach from Notebook
def baseline_approach(df):
    X = df.copy()
    for col_name in ['CODSTATUSCLIENTE', 'CODRATING_CEDENTE']:
        if col_name in X.columns:
            X[col_name] = X[col_name].astype('category')
    return X

# Tensor optimization approach: Select dtype float64 -> float32 and then category
def tensor_approach(df):
    X = df.copy()

    # Downcast float64 to float32
    float64_cols = X.select_dtypes(include=['float64']).columns
    if len(float64_cols) > 0:
        X[float64_cols] = X[float64_cols].astype('float32')

    for col_name in ['CODSTATUSCLIENTE', 'CODRATING_CEDENTE']:
        if col_name in X.columns:
            X[col_name] = X[col_name].astype('category')
    return X

# Benchmarking
start_time = time.time()
df_base = baseline_approach(df_pd)
base_time = time.time() - start_time
print(f"Baseline Time: {base_time:.4f}s")
print(f"Baseline RAM: {df_base.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

start_time = time.time()
df_tensor = tensor_approach(df_pd)
tensor_time = time.time() - start_time
print(f"Tensor Time: {tensor_time:.4f}s")
print(f"Tensor RAM: {df_tensor.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
