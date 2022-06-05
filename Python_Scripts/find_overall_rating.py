import pandas as pd

# Load Dataframe
data = pd.read_csv(
    "/home/leonidastsant/Downloads/MovieLens_Dataset/rating_test.csv")

data.drop('userId', inplace=True, axis=1)  # Delete userId column

mean_rating = data.groupby("movieId", as_index=False).mean().round(1)

# print(mean_rating.rating)
print(mean_rating)
