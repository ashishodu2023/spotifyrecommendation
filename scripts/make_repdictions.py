from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import MaxAbsScaler
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
import pandas as pd


df = df_stream
assembler = VectorAssembler(inputCols = pca_col, outputCol = 'features')
df1 = assembler.transform(df.na.drop("any"))

scaler = StandardScaler(
    inputCol = 'features', 
    outputCol = 'scaledFeatures',
    withMean = True,
    withStd = True
).fit(df1)


scaledData = scaler.transform(df1)

a = scaledData.select('scaledFeatures')

pca = PCA(k=10,inputCol="scaledFeatures", outputCol="pcaFeatures").fit(scaledData)

df_pca = pca.transform(scaledData)

df3 = df_pca.select('pcaFeatures')

# Trains a k-means model.
kmeans = KMeans(k=4)
model = kmeans.fit(df_pca)

# Make predictions
predictions = model.transform(df_pca)



from  pyspark.ml.linalg import Vectors
temp = predictions.select("pcaFeatures")
temp = temp.rdd.map(lambda x: [float(y) for y in x['pcaFeatures']]).toDF(['PC1'])




pandas_df = temp.toPandas()
predictions_df = predictions.toPandas()

pred = predictions_df['prediction']

final_df = pd.concat([pandas_df,pred],axis =1)

df_pca_res = spark.createDataFrame(final_df)

df_pca_res.show(5)
