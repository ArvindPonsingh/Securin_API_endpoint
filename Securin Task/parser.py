import pandas as pd
import json
import ast
import sqlite3
import psycopg2
from sqlalchemy import create_engine
# Load JSON data from file
with open('US_recipes_null.Pdf.json', 'r') as f:
    data = json.load(f)
    if isinstance(data, dict):
        data = [data]
    elif isinstance(data, str):
        data = ast.literal_eval(data)

# Create initial DataFrame
df = pd.DataFrame(data)
extracted_data = []

for col in df.columns:
    recipe_data = df[col].iloc[0]
    extracted_data.append(recipe_data)

expanded_df = pd.DataFrame(extracted_data)
df = expanded_df

df['cuisine'] = df['cuisine'].astype(str)
df['title'] = df['title'].astype(str)
df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
df['prep_time'] = pd.to_numeric(df['prep_time'], errors='coerce').astype('Int64')
df['cook_time'] = pd.to_numeric(df['cook_time'], errors='coerce').astype('Int64')
df['total_time'] = pd.to_numeric(df['total_time'], errors='coerce').astype('Int64')
df['description'] = df['description'].astype(str)
df['nutrients'] = df['nutrients'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
df['serves'] = df['serves'].astype(str)
columns_to_keep = ['cuisine', 'title', 'rating', 'prep_time', 'cook_time', 'total_time', 'description', 'nutrients', 'serves']
df = df[columns_to_keep]


db_user = 'postgres'
db_password = 1234
db_host = 'localhost'
db_port = '5432'
db_name = 'Local_db'


engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
df.to_sql('recipes', engine, if_exists='replace', index=False)

print("DataFrame saved to PostgreSQL table 'recipes'.")