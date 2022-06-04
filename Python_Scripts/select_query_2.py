from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
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
movie_data = pd.read_csv(
    "/home/leotsant/Downloads/MovieLens_Dataset/movie_test.csv")
tag_data = pd.read_csv(
    "/home/leotsant/Downloads/MovieLens_Dataset/tag_test.csv")
rating_data = pd.read_csv(
    "/home/leotsant/Downloads/MovieLens_Dataset/rating_test.csv")


tag_data.drop(['userId', 'timestamp'], inplace=True, axis=1)
rating_data.drop(['userId', 'timestamp'], inplace=True, axis=1)


movieId_with_tags = tag_data.groupby(
    "movieId", as_index=True).transform(lambda x: ','.join(x))

print(movieId_with_tags)

if row:
    print("Success!")
else:
    print("An error occurred.")
