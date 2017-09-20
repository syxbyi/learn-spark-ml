#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'syxbyi'
__version__ = '1.0.0'

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

# basis to use spark sql
spark = SparkSession.builder.getOrCreate()

# one column dataframe
data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
        (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
        (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
        (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]
df1 = spark.createDataFrame(data, ["features"])

# two column dataframe
df2 = spark.createDataFrame([
    (0, Vectors.dense([1.0, 0.5, -1.0]),),
    (1, Vectors.dense([2.0, 1.0, 1.0]),),
    (2, Vectors.dense([4.0, 10.0, 2.0]),)
], ["id", "features"])

# display a dataframe
df1.show()
df2.show()

# read a csv file
dataframe = spark.read.csv('example.csv', header = True)
dataframe.show()
