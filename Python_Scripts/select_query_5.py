from cassandra.cluster import Cluster
import pandas as pd
import time

cluster = Cluster()
session = cluster.connect('big_data_project')

start_time = time.time()

query_1 = " SELECT * FROM movie_id_by_tag WHERE tag = 'comedy' "

# Returns DataFrame object from query 1
data = pd.DataFrame(list(session.execute(query_1)))

new_df = pd.DataFrame(dict(movieId=pd.Series([], dtype=int), average_rating=pd.Series([], dtype=float)))

query_2 = "SELECT avg(rating) AS average FROM rating_by_movie_id WHERE movieid = %s"
futures = []
i = 0

# Executes asynchronously all the select statements
while i < len(data):
        futures.append(session.execute_async(query_2, [data.movieid[i]]))
        i += 1
         
i=0
for future in futures:
    # Try except block because some movies might not have ratings
    try:
        # Returns DataFrame Cassandra's Result Set
        rows = pd.DataFrame(list(future.result()))

        row_to_insert = {'movieId': data.movieid[i], 'average_rating': rows.average[0].round(1)}

        new_df = new_df.append(row_to_insert, ignore_index = True)

        i += 1

    except:
        i += 1
        continue 

sorted_data = new_df.sort_values(by=['average_rating'], ascending=False).head(20).reset_index(drop=True)

# Selects the titles for the 20 movies
query_3 = "SELECT title FROM movie WHERE movieid = ?"
prepared_query_3 = session.prepare(query_3)

titles = []
j = 0 
while j < len(sorted_data):
    res = pd.DataFrame(list(session.execute(prepared_query_3, [int(sorted_data.movieId[j])])))
    titles.append(res.title[0])
    j += 1

# Adds new column 'Title'
sorted_data.insert(loc=1, column='Title', value=titles)

sorted_data['movieId'] = sorted_data['movieId'].astype(int)
# print(sorted_data)

print(sorted_data)

elapsedTime = round((time.time() - start_time), 2)

print(f"Elapsed time: {elapsedTime} seconds")
