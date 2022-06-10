from cassandra.cluster import Cluster
import pandas as pd
import time

start_time = time.time()

cluster = Cluster()
session = cluster.connect('big_data_project')

start_time = time.time()

query_1 = "SELECT rating,movieId FROM rating_by_timestamp WHERE month_by_year ='2015-01' AND timestamp >= '2015-01-01' AND timestamp <= '2015-01-15' "

df = pd.DataFrame(list(session.execute(query_1)))

# Groups data by movieId, calculates the average rating and sorts list
res = df.groupby("movieid", as_index=False).mean().round(1).sort_values(by=['rating'], ascending=False)

# Keeps first 30 rows and resets Pandas DataFrame index
res_30 = res.head(30).reset_index(drop=True)


# Selects the titles for the 30 movies
query_2 = "SELECT title FROM movie WHERE movieid = ?"
prepared_query_2 = session.prepare(query_2)

titles = []
j = 0 
while j < len(res_30):
    ans = pd.DataFrame(list(session.execute(prepared_query_2, [res_30.movieid[j]])))
    titles.append(ans.title[0])
    j += 1

# Adds new column 'Title'
res_30.insert(loc=1, column='Title', value=titles)

print(res_30)
  
elapsedTime = round((time.time() - start_time), 2)

print(f"Elapsed time: {elapsedTime} seconds")

