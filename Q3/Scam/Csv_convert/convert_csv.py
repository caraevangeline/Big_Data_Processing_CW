import pandas as pd
df = pd.read_json('input.json')
df.to_csv('input.csv')
