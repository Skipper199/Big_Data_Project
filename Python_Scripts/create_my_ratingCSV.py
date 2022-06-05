import pandas as pd
import time

CHUNKSIZE = 1_000

start_time = time.time()

df = pd.read_csv("/home/leonidastsant/Downloads/MovieLens_Dataset/rating.csv", chunksize=CHUNKSIZE)

i = 0

df["month_by_year"] = df.timestamp.str[:7]

df['timestamp'] = pd.to_datetime(df['timestamp'])

df.to_csv("/home/leonidastsant/Downloads/MovieLens_Dataset/my_rating.csv", index=False)

elapsedTime = time.time() - start_time
print("Done!")
print(f"Elapsed time: {elapsedTime}")