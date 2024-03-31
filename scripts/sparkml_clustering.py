from pyspark.ml.feature import VectorAssembler
assembler=VectorAssembler(inputCols=[
 'danceability',
 'energy',
 'loudness',
 'speechiness',
 'acousticness',
 'instrumentalness',
 'liveness',
 'valence',
 'tempo'], outputCol='features')
assembled_data=assembler.setHandleInvalid("skip").transform(df)

from pyspark.ml.feature import StandardScaler
scale=StandardScaler(inputCol='features',outputCol='standardized')
data_scale=scale.fit(assembled_data)
df=data_scale.transform(assembled_data)

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
silhouette_score=[]
evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized', \
                                metricName='silhouette', distanceMeasure='squaredEuclidean')


KMeans_algo=KMeans(featuresCol='standardized', k=3)
    
KMeans_fit=KMeans_algo.fit(df)
    
output_df =KMeans_fit.transform(df)
  
import numpy as np, pandas as pd
import matplotlib.pyplot as plt, seaborn as sns
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

class SpotifyRecommender():
    def __init__(self, rec_data):
        self.rec_data_ = rec_data
    
    def spotify_recommendations(self, song_name, amount=1):
        distances = []
        song = self.rec_data_[(self.rec_data_.name.str.lower() == song_name.lower())].head(1).values[0]
        # get details of our fav song from name we pass as x earlier.
        res_data = self.rec_data_[self.rec_data_.name.str.lower() != song_name.lower()]
        #dropping the data with our fav song so that it doesnt affect our recommendation.
        for r_song in tqdm(res_data.values):
            # tqdm is just used for showing the bar of iteration through our streamed songs.
            dist = 0
            for col in np.arange(len(res_data.columns)):
                # (len(res_data.columns) gets us the number of columns -> 13 in our case.
                #indeces of non-numerical columns neednt be considered.
                if not col in [0,1,13]:
                    #calculating the manhettan distances for each numerical feature
                    # song -> from our fav dataset.
                    # r_song -> from streaming data.
                    dist = dist + np.absolute(float(song[col]) - float(r_song[col]))
            distances.append(dist)
            # distances are calculated and appended and added to a new column called distances in our dataset.
        res_data['distance'] = distances
        #sorting our data to be ascending by 'distance' feature
        res_data = res_data.sort_values('distance')
        # resulting dataset have the song similar to our fav song's numerical values and thus recommended.
        columns = ['name', 'artists', 'acousticness', 'liveness', 'instrumentalness', 'energy', 'danceability', 'valence']
        return res_data[columns][:amount]
    
    
datad = output_df.select('name',
 'artists',
 'danceability',
 'energy',
 'key',
 'loudness',
 'speechiness',
 'acousticness',
 'instrumentalness',
 'liveness',
 'valence',
 'tempo',
 'prediction')



datf = datad.toPandas()
datf.drop(datf[datf['artists'] == '0'].index, inplace = True)
datf.drop_duplicates(inplace=True)
datf.drop(datf[datf['danceability'] == 0.0000].index, inplace = True)
datf.drop(datf[datf['liveness'] == 0.000].index, inplace = True)
datf.drop(datf[datf['instrumentalness'] == 0.000000].index, inplace = True)
datf.drop(datf[datf['energy'] == 0.0000].index, inplace = True)
datf.drop(datf[datf['danceability'] == 0.000].index, inplace = True)
datf.drop(datf[datf['valence'] == 0.000].index, inplace = True)

y = datf
value_pred = datf.iloc[-1:]['prediction']
#datf = datf[datf['prediction'] == list(value_pred)[0]]

recommender = SpotifyRecommender(datf)
x = add_df['name'].tolist()[0]

rec_song = recommender.spotify_recommendations(x, 10)

v = add_df[['name', 'artists',  'acousticness', 'liveness', 'instrumentalness', 'energy', 
       'danceability', 'valence']]

rec_song = pd.concat([rec_song, v])
rec_song.to_csv('rec_song.csv')
