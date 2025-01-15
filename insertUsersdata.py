import numpy as np
import pandas as pd
import os
from cassandra.cluster import Cluster
df = pd.read_csv('hdfs://localhost:9000/spark/final_data2.csv')
df = df[['user_id', 'user_name']]
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('courses')
count = 0
os.system("clear")
for index, row in df.iterrows():
    if count == 10:
        break
    rows = session.execute(
        """
        SELECT user_id
        FROM usersInfo
        WHERE user_id=%s
        """,
        (row['user_id'],)
    )
    if rows:
        print(f"User ID: {row['user_id']} already exists in the database!")
        continue
    print(f"Inserting User ID: {row['user_id']} into the database...")
    session.execute(
        """
        INSERT INTO usersInfo (user_id, display_name)
        VALUES (%s, %s)
        """,
        (row['user_id'], row['user_name'])
    )
    count += 1