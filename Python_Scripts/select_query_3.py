from cassandra.cluster import Cluster
import pandas as pd
import time

cluster = Cluster()
session = cluster.connect('big_data_project')

start_time = time.time()
 
query = "SELECT * FROM genre_by_movie_id WHERE genre = 'Adventure' "

# Returns DataFrame object from query 1
data = pd.DataFrame(list(session.execute(query))) 

elapsedTime = time.time() - start_time

print(data)
print(f"Elapsed time: {elapsedTime}")