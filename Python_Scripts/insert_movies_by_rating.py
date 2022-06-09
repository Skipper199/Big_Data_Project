from cassandra.cluster import Cluster
import pandas as pd

cluster = Cluster()
session = cluster.connect('big_data_project')




# Read Files
rating_data = pd.read_csv(
    "/home/leonidastsant/Downloads/MovieLens_Dataset/rating.csv")


time_bucket = rating_data.timestamp

# Converts timestamp column from string to datetime
rating_data['timestamp'] = pd.to_datetime(rating_data['timestamp'])


query = "INSERT INTO movies_by_rating (movie_id, rating, user_id, timestamp, date_bucket) VALUES (?,?,?,?,?) "
prepared = session.prepare(query)
#prepared.consistency_level = ConsistencyLevel.QUORUM

j=0
while j < 100:
    

    session.execute_async(
        prepared, (rating_data.movieId[j], rating_data.rating[j], rating_data.userId[j], rating_data.timestamp[j], time_bucket[j][0:7]))
    j += 1

print("Success!")
