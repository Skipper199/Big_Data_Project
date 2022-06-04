from tracemalloc import start
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import time

cloud_config = {
    'secure_connect_bundle': '/home/leotsant/Downloads/secure-connect-big-data-project-cluster.zip'
}
auth_provider = PlainTextAuthProvider(
    'MyrrlmamdWZRMrQhSwUIeyNx', 'KzmZYIBZ4p0K_8RYYdldNqlA1rHs-l-LGEcG.lSSSFsqcwbR9siBZmF6jWW5C9s.Bh+zz3ZGjJvYnc,Rt-QiziY+KWul0Yr9u_mNnpUXMoynR+6TWuqvoDhcqER+SMNr')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect('big_data')

row = session.execute("select release_version from system.local").one()

start_time = time.time()

query = "SELECT rating,movie_id FROM movies_by_rating WHERE date_bucket ='2015-01' AND timestamp >= '2015-01-01' AND timestamp <= '2015-01-15' "
df = pd.DataFrame(list(session.execute(query)))

elapsedTime = time.time() - start_time

# Groups data by movie_id, calculates the mean rating and sorts list
res = df.groupby("movie_id", as_index=False).mean().round(
    1).sort_values(by=['rating'], ascending=False)


res_without_index = res.to_string(index=False)

print(res_without_index)
print(f"Elapsed time: {elapsedTime}")


if row:
    print("Success")
else:
    print("An error occurred.")
