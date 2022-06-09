import pandas as pd

tag_df = pd.read_csv("/home/leonidastsant/Downloads/MovieLens_Dataset/tag_test.csv")

movie_df = pd.read_csv("/home/leonidastsant/Downloads/MovieLens_Dataset/movie.csv")

data = tag_df.drop(["userId", "timestamp"], axis=1)

# data["tag"] = data["tag"].astype(str)

# a = data.groupby("movieId").agg({"tag" : ','.join})

i = 0
while i < len(tag_df.movieId):
    tag_df["title"][i] = movie_df.title

a = data.groupby("movieId").count()

print()