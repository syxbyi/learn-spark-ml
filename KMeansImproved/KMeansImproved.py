#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'syxbyi'
__version__ = '1.0.0'

from pyspark.sql import SparkSession
import pyspark.sql.functions as sqlf
from pyspark.ml.clustering import KMeans
# Import Normalizer here
from pyspark.ml.feature import VectorAssembler, Normalizer

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('nolabel_rm_num_5_precent_kddcup.data')
#df.show(5)

vecAssembler = VectorAssembler(inputCols = df.columns, outputCol = 'features')
# convert string to double
cast = [sqlf.col(x).cast('Double').alias(x) for x in vecAssembler.getInputCols()]
df = df.select(*cast)
#print(df.columns)
df = vecAssembler.transform(df).select('features')
df.select('features').show(5, False)

# Normalizing using L1 norm
normalizer = Normalizer(inputCol = 'features', outputCol = 'norm_features', p = 1.0)
norm_df = normalizer.transform(df)
# Show the features column and do not omit
#norm_df.select('norm_features').show(5, False)

# Change normalizing method
#infnorm_df = normalizer.transform(df, {normalizer.p: float('inf')})
#infnorm_df.select('features').show(5, False)

kmeans = KMeans(k = 2, seed = 1, featuresCol = 'norm_features')
model = kmeans.fit(norm_df)

wssse = model.computeCost(norm_df)
print("Within Set Sum of Squared Errors = " + str(wssse))

centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
