from tracemalloc import start
from cassandra.cluster import Cluster
import pandas as pd
import time

start_time = time.time()

cluster = Cluster()
session = cluster.connect('big_data_project')

start_time = time.time()

query = "SELECT rating,movieId FROM rating_by_timestamp WHERE month_by_year ='2015-01' AND timestamp >= '2015-01-01' AND timestamp <= '2015-01-15' "

df = pd.DataFrame(list(session.execute(query)))

# Groups data by movieId, calculates the average rating and sorts list
res = df.groupby("movieid", as_index=False).mean().round(1).sort_values(by=['rating'], ascending=False)

# Keeps first 30 rows and removes index from Pandas DataFrame
res_without_index = res.head(30).to_string(index=False)

elapsedTime = time.time() - start_time

print(res_without_index)
  

print(f"Elapsed time: {elapsedTime}")

print("Success")
