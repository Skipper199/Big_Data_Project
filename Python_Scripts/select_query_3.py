from cassandra.cluster import Cluster
import pandas as pd
import time

cluster = Cluster()
session = cluster.connect('big_data_project')

start_time = time.time()
 
# In order for the query to work we have to first create an index. If index already exists than below command is not needed.
# session.execute("CREATE CUSTOM INDEX genre_index ON big_data_project.movie (genres) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS'}") 

query = "SELECT * FROM movie WHERE genres LIKE '%Adventure%' "

# Returns DataFrame object from query 1
data = pd.DataFrame(list(session.execute(query))) 

elapsedTime = time.time() - start_time

print(data)
print(f"Elapsed time: {elapsedTime}")