from cassandra.cluster import Cluster
import pandas as pd
import time

cluster = Cluster()
session = cluster.connect('big_data_project')

start_time = time.time()

query_1 = "SELECT * FROM genre_by_movie_id WHERE title = 'Jumanji (1995)' ALLOW FILTERING"

# Returns DataFrame object from query 1
movie_df = pd.DataFrame(list(session.execute(query_1))) 

query_2 = "SELECT rating FROM rating_by_movie_id WHERE movieid = ?"
prepared_query_2 = session.prepare(query_2)

# Returns DataFrame object from query 2
rating_df = pd.DataFrame(list(session.execute(prepared_query_2, [movie_df.movieid[0]])))
 
# Calculates average rating
average_rating = rating_df.mean().round(1)

query_3 = "SELECT tag FROM tag_by_movie_id WHERE movieid = ?"
prepared_query_3 = session.prepare(query_3)

# Returns DataFrame object from query 3
tag_df = pd.DataFrame(list(session.execute(prepared_query_3,[movie_df.movieid[0]])))

# Finds the top 5 tags (with the most occurencies)
top_5_tags = tag_df['tag'].value_counts().index.tolist()[:5]

genres = []
i = 0
while i < len(movie_df):
    genres.append(movie_df.genre[i])
    i += 1

# Prints desired output
print(f"Movie ID: {movie_df.movieid[0]}\nTitle: {movie_df.title[0]}\nGenres: {genres}\nRating: {average_rating[0]}\nTop Tags:{top_5_tags}")

elapsedTime = time.time() - start_time

print(f"Elapsed time: {elapsedTime}")