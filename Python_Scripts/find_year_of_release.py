import re
import pandas as pd

data = pd.read_csv("/home/leotsant/Downloads/MovieLens_Dataset/movie.csv")


i = 0

year_of_release = []

while i < len(data.movieId):
    # RegEx to keep only the last parenthesis
    year_of_release.append(re.findall("\(([^()]*)\)$", data.title[i]))
    i = i + 1


flat_year_of_release = [
    item for sublist in year_of_release for item in sublist]  # Flattens list

# results = [int(i) for i in flat_year_of_release] Converts list of strings to integers

print(flat_year_of_release)
