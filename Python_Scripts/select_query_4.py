from cassandra.cluster import Cluster
import pandas as pd
import time

cluster = Cluster()
session = cluster.connect('big_data_project')

start_time = time.time()
 

# In order for the query to work we have to first create an index. If index already exists than below command is not needed.
# session.execute("CREATE CUSTOM INDEX title_index ON big_data_project.movie (title) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS'}")

query = " SELECT * FROM movie WHERE title LIKE '%star%' "

# Returns DataFrame object from query
data = pd.DataFrame(list(session.execute(query))) 

# Reorders columns
column_names = ["movieid", "title", "genres", "year_of_release"]
data = data.reindex(columns=column_names)

print(data)

elapsedTime = round((time.time() - start_time), 2)
print(f"Elapsed time: {elapsedTime} seconds")
