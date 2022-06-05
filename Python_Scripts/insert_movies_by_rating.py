from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, SimpleStatement, BatchType
from cassandra.policies import RetryPolicy
import pandas as pd

cluster = Cluster()
session = cluster.connect('big_data_project')

row = session.execute("select release_version from system.local").one()


# Read Files
rating_data = pd.read_csv(
    "/home/leonidastsant/Downloads/MovieLens_Dataset/rating_test.csv")

time_bucket = rating_data.timestamp

# Converts timestamp column from string to datetime
rating_data['timestamp'] = pd.to_datetime(rating_data['timestamp'])

query = "INSERT INTO movies_by_rating (movie_id, rating, user_id, timestamp, date_bucket) VALUES (?,?,?,?,?) IF NOT EXISTS"
prepared = session.prepare(query)
#prepared.consistency_level = ConsistencyLevel.QUORUM

j = 0
while j < len(rating_data.movieId):
    session.execute_async(
        prepared, (rating_data.movieId[j], rating_data.rating[j], rating_data.userId[j], rating_data.timestamp[j], time_bucket[j][0:7]))
    j += 1

if row:
    print("Success!")
else:
    print("An error occurred.")
