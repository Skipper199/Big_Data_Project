from tracemalloc import start
from cassandra.cluster import Cluster
import pandas as pd
import time

cluster = Cluster()
session = cluster.connect('big_data_project')

row = session.execute("select release_version from system.local").one()

start_time = time.time()

query = "SELECT rating,movieId FROM movies_by_rating WHERE month_by_year ='2015-01' AND timestamp >= '2015-01-01' AND timestamp <= '2015-01-15' "

df = pd.DataFrame(list(session.execute(query)))

elapsedTime = time.time() - start_time

# Groups data by movieId, calculates the mean rating and sorts list
res = df.groupby("movieid", as_index=False).mean().round(1).sort_values(by=['rating'], ascending=False)

# Keeps first 30 rows and removes index from Pandas DataFrame
res_without_index = res.head(30).to_string(index=False)
print(res_without_index)
  

print(f"Elapsed time: {elapsedTime}")


if row:
    print("Success")
else:
    print("An error occurred.")
