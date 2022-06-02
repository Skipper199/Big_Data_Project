from cassandra.cluster import Cluster
cluster = Cluster()

session = cluster.connect('big_data')

row = session.execute("select release_version from system.local").one()

session.execute("INSERT INTO movie (movie_id, year_of_release, genre, overall_rating, title, tags) VALUES (2, 2001, 'horror', 5, 'test_movie_2', 'test_tag_2')")

print ("Success")
