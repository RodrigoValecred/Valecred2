import pandas as pd
import numpy as np
import timeit

# Create a sample DataFrame
np.random.seed(42)
df = pd.DataFrame({'excesso_valor': np.random.randn(1000000)})

# Function with apply
def apply_func():
    df_copy = df.copy()
    df_copy['excesso_valor'] = df_copy['excesso_valor'].apply(lambda x: x if x > 0 else 0)

# Function with np.where
def where_func():
    df_copy = df.copy()
    df_copy['excesso_valor'] = np.where(df_copy['excesso_valor'] > 0, df_copy['excesso_valor'], 0)

def clip_func():
    df_copy = df.copy()
    df_copy['excesso_valor'] = df_copy['excesso_valor'].clip(lower=0)


print("Apply execution time: ", timeit.timeit(apply_func, number=10))
print("Where execution time: ", timeit.timeit(where_func, number=10))
print("Clip execution time: ", timeit.timeit(clip_func, number=10))
