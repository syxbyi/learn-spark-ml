#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'syxbyi'
__version__ = '1.0.0'

from pyspark.sql import SparkSession
import pyspark.sql.functions as sqlf
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('nolabel_rm_num_5_precent_kddcup.data')
#df.show(5)

# convert string to double
cast = [sqlf.col(x).cast('Double').alias(x) for x in vecAssembler.getInputCols()]
df = df.select(*cast)
#print(df.columns)
vecAssembler = VectorAssembler(inputCols = df.columns, outputCol = 'features')
df = vecAssembler.transform(df)
#df.show(5)

kmeans = KMeans(k = 2, seed = 1)
model = kmeans.fit(df)

wssse = model.computeCost(df)
print("Within Set Sum of Squared Errors = " + str(wssse))

centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
