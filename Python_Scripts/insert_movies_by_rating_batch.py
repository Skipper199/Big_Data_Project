from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, SimpleStatement, BatchType
from cassandra.policies import RetryPolicy
import pandas as pd

cloud_config = {
    'secure_connect_bundle': '/home/leotsant/Downloads/secure-connect-big-data-project-cluster.zip'
}
auth_provider = PlainTextAuthProvider(
    'MyrrlmamdWZRMrQhSwUIeyNx', 'KzmZYIBZ4p0K_8RYYdldNqlA1rHs-l-LGEcG.lSSSFsqcwbR9siBZmF6jWW5C9s.Bh+zz3ZGjJvYnc,Rt-QiziY+KWul0Yr9u_mNnpUXMoynR+6TWuqvoDhcqER+SMNr')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect('big_data')

row = session.execute("select release_version from system.local").one()


# Read Files
rating_data = pd.read_csv(
    "/home/leotsant/Downloads/MovieLens_Dataset/rating_test.csv")

rating_data['timestamp'] = pd.to_datetime(rating_data['timestamp'])

insert_user = SimpleStatement(
    "INSERT INTO movies_by_rating (movie_id, rating, user_id, timestamp) VALUES (%s,%s,%s,%s)")
batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

j = 0
i = 0
while j < len(rating_data.movieId):
    i += 1
    while (i % 100 != 0):
        batch.add(insert_user, (rating_data.movieId[i-1], rating_data.rating[i-1],
                  rating_data.userId[i-1], rating_data.timestamp[i-1]))
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
