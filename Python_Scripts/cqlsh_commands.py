copy_rating_by_timestamp = "COPY big_data_project.rating_by_timestamp (movieId, rating, timestamp, month_by_year) FROM '/home/leonidastsant/Downloads/MovieLens_Dataset/ratingTimestamp.csv' WITH HEADER=TRUE;"

copy_rating_by_movie_id = "COPY big_data_project.rating_by_movie_id (userid, movieId, rating) FROM '/home/leonidastsant/Downloads/MovieLens_Dataset/ratingMovieID.csv' WITH HEADER=TRUE;"

copy_tag_by_movie_id = "COPY big_data_project.tag_by_movie_id(userid, movieid, tag) FROM '/home/leonidastsant/Downloads/MovieLens_Dataset/tagMovieID.csv' WITH HEADER=TRUE;" 

############################### NOT IN USE ##################################
copy_genre_by_movie_id = "COPY big_data_project.genre_by_movie_id(movieid, title, genre, year_of_release) FROM '/home/leonidastsant/Downloads/MovieLens_Dataset/genreByMovie.csv' WITH HEADER=TRUE;" 

copy_movie = "COPY big_data_project.movie(movieid, title, genres, year_of_release) FROM '/home/leonidastsant/Downloads/MovieLens_Dataset/movie_info.csv' WITH HEADER=TRUE;" 