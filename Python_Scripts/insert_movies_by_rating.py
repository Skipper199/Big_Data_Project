from cassandra.cluster import Cluster
import pandas as pd

print("dada")
cluster = Cluster()
session = cluster.connect('big_data_project')
print("start")

row = session.execute("select release_version from system.local").one()


# Read Files
print("before read")
rating_data = pd.read_csv(
    "/home/leonidastsant/Downloads/MovieLens_Dataset/rating.csv")
print("after read")

time_bucket = rating_data.timestamp

# Converts timestamp column from string to datetime
rating_data['timestamp'] = pd.to_datetime(rating_data['timestamp'])
print("timestamp")

query = "INSERT INTO movies_by_rating (movie_id, rating, user_id, timestamp, date_bucket) VALUES (?,?,?,?,?) "
prepared = session.prepare(query)
#prepared.consistency_level = ConsistencyLevel.QUORUM

print("before while")
j = 0
while j < 100:
    print("in while")

    session.execute_async(
        prepared, (rating_data.movieId[j], rating_data.rating[j], rating_data.userId[j], rating_data.timestamp[j], time_bucket[j][0:7]))
    j += 1

print("after while")

if row:
    print("Success!")
else:
    print("An error occurred.")
