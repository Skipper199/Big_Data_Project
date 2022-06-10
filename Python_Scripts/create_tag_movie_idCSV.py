import pandas as pd

df = pd.read_csv("/home/leonidastsant/Downloads/MovieLens_Dataset/tag.csv")

data = df.drop(["userId", "timestamp"], axis=1)

data.to_csv("/home/leonidastsant/Downloads/MovieLens_Dataset/tagMovieID.csv", index=False)