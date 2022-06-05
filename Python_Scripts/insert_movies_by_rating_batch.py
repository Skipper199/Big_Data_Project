from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, SimpleStatement, BatchType
from cassandra.policies import RetryPolicy
import pandas as pd

cluster = Cluster()
session = cluster.connect('big_data_project')

row = session.execute("select release_version from system.local").one()


# Read Files
rating_data = pd.read_csv(
    "/home/leonidastsant/Downloads/MovieLens_Dataset/rating.csv")

time_bucket = rating_data.timestamp

rating_data['timestamp'] = pd.to_datetime(rating_data['timestamp'])

insert_user = SimpleStatement(
    "INSERT INTO movies_by_rating (movie_id, rating, user_id, timestamp, date_bucket) VALUES (%s,%s,%s,%s,%s)")
batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

j = 0
i = 0
while j < len(rating_data.movieId):
    i += 1
    while (i % 100 != 0):
        batch.add(insert_user, (rating_data.movieId[i-1], rating_data.rating[i-1],
                  rating_data.userId[i-1], rating_data.timestamp[i-1], time_bucket[i-1][0:7]))
        i += 1
        j += 1
        if j == len(rating_data.movieId):
            break
    session.execute_async(batch)
    batch.clear()


if row:
    print("Success!")
else:
    print("An error occurred.")
