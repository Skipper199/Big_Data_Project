from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
import pandas as pd

cluster = Cluster()
session = cluster.connect('big_data_project')

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
