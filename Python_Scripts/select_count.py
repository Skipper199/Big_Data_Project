from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd

cluster = Cluster()
session = cluster.connect('big_data_project')

row = session.execute("select release_version from system.local").one()

df = pd.DataFrame(list(session.execute(
    "SELECT count(*) FROM movies_by_rating")))

print(df)

if row:
    print("Success")
else:
    print("An error occurred.")
