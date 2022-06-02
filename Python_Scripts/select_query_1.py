from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd

cloud_config = {
    'secure_connect_bundle': '/home/leotsant/Downloads/secure-connect-big-data-project-cluster.zip'
}
auth_provider = PlainTextAuthProvider(
    'MyrrlmamdWZRMrQhSwUIeyNx', 'KzmZYIBZ4p0K_8RYYdldNqlA1rHs-l-LGEcG.lSSSFsqcwbR9siBZmF6jWW5C9s.Bh+zz3ZGjJvYnc,Rt-QiziY+KWul0Yr9u_mNnpUXMoynR+6TWuqvoDhcqER+SMNr')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect('big_data')

row = session.execute("select release_version from system.local").one()

df = pd.DataFrame(list(session.execute(
    "SELECT rating,movie_id FROM movies_by_rating WHERE timestamp >= '2015-01-01' AND timestamp <= '2015-01-15' ALLOW FILTERING")))

# Groups data by movie_id, calculates the mean rating and sorts list
res = df.groupby("movie_id", as_index=False).mean().round(
    1).sort_values(by=['rating'], ascending=False)

res_without_index = res.to_string(index=False)

print(res_without_index)

if row:
    print("Success")
else:
    print("An error occurred.")
