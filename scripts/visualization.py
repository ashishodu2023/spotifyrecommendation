pca_col = [
 'danceability',
 'energy',
 'key',
 'loudness',
 'speechiness',
 'acousticness',
 'instrumentalness',
 'liveness',
 'valence',
 'tempo']

from pyspark_dist_explore import hist
import matplotlib.pyplot as plt
import seaborn as sns
x = df_stream.na.drop("any")
x1 = x.toPandas()

plt.figure(figsize=(16, 8))
sns.set(style="whitegrid")
y = x1.groupby("name")["popularity"].mean().sort_values(ascending=False).head(10)
ax = sns.barplot(y.index, y)
ax.set_title('Top Tracks with Popularity')
ax.set_ylabel('Popularity')
ax.set_xlabel('Tracks')
ax.set_ylim(70,100)
plt.xticks(rotation = 90)

plt.figure(figsize=(16, 8))
sns.set(style="whitegrid")
y2 = x1.groupby("artists")["popularity"].mean().sort_values(ascending=False).head(10)
ax = sns.barplot(y2.index,y2)
ax.set_title('Top Artists vs Popularity')
ax.set_ylabel('Popularity')
ax.set_xlabel('Artists')
ax.set_ylim(70,100)
plt.xticks(rotation = 90)

import plotly.express as px
fig = px.scatter_3d(final_df, x="PC1", y="_2",z ="_3", color="prediction")
fig.show()
