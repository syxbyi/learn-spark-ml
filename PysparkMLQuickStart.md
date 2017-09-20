这是一篇快速入门教程：实现基于KMeans算法的机器学习，使用Python语言，应用spark分布式计算框架中的ml库。

这篇文章讲了pyspark编程, 和machine learning, 所以你需要:

1. 了解一点spark, 知道它是什么, 是干什么的. 会配置spark环境, 会运行spark程序.
2. 会一点编程, 会一点Python语言, 会查Python API文档.

相关:

1. [spark环境配置](SparkBuild.md)
2. [spark基本概念](SparkQuickStart.md)

# 一点历史

Spark有两个机器学习库，一个叫MLlib，一个叫ml。

> MLlib: RDD-based API

> ml: DataFrame-based API

MLlib是比较早的API，ml是新的API。spark官方推荐我们使用ml，而目前的spark官方教程也是主要面向ml这个库的。

下面是摘自官网的详细比较：

> DataFrames provide a more user-friendly API than RDDs. 

> 比起基于RDD的MLlib，ml提供的接口更加友好。

> The DataFrame-based API for MLlib provides a uniform API across ML algorithms and across multiple languages.

> 对于不同语言和学习算法，ml库提供了一致的接口命名。

> DataFrames facilitate practical ML Pipelines, particularly feature transformations. 

> DataFrame支持管道。

本教程中，我们也按照官网上的推荐，使用ml库。

# 一点Spark SQL & RDD & DataFrame

> One use of Spark SQL is to execute SQL queries. Spark SQL can also be used to read data from an existing Hive installation. 

但是在我们的机器学习过程中，既不需要SQL，也不需要Hive，那么为什么要使用Spark SQL呢？--因为我们要使用一个名叫`DataFrame`的数据类型，这个数据类型在Spark SQL中实现。

如果你看过旧版的spark机器学习教程(一般中文的都比较旧)，那些教程讲的一般都是使用MLlib和RDD数据类型。但在新的ml库中，流通的数据类型是基于RDD的更高层抽象(也是优化)：DataFrame类型。而DataFrame类型基于Spark SQL库。

## 使用Spark SQL库

如果你使用RDD数据类型(而不是DataFrame)，你可能见过类似这样的一段代码：

```py
# 这段代码是用来创建RDD的, 如果使用DataFrame，这段代码就不需要了
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
```

这段代码创建了一个spark环境，通过`sc`对象，才能获取一个RDD。但是如果你使用DataFrame，就不需要这段代码了。

虽然不用创建SparkContext对象了, 但是我们还是要创建另一个对象--SparkSession。

> With a SparkSession, applications can create DataFrames from an existing RDD, from a Hive table, or from Spark data sources.

根据官方文档的描述，我们可以通过这个SparkSession对象，直接创建一个DataFrames。

首先，用以下代码创建一个SparkSession。如果你使用spark交互式shell, 就不需要创建SparkSession, 可以直接使用`spark`对象(就像可以直接使用`sc`对象那样). 

```py
from pyspark.sql import SparkSession
appname = 'name'
spark = SparkSession.builder.appName(appname).getOrCreate()
```

然后，就可以像这样直接读入一个json文件(这就创建了一个DataFrame)，然后把内容打印出来。(这只是举个读取文件的例子，其实我们并不需要json文件。)

```py
# spark is an existing SparkSession
df = spark.read.json("examples/src/main/resources/people.json")
# Displays the content of the DataFrame to stdout
df.show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+
```

# 使用各种data source创建DataFrame

## 向量创建DataFrame

向量是在ml的linalg子模块中的数据结构. 可以像这样创建一些向量: 

```py
from pyspark.ml.linalg import Vectors

# 创建一个稠密向量
dv = Vectors.dense([4.0, 10.0, 2.0])

# 创建一个稀疏向量
sv1 = Vectors.sparse(4, {1: 1.0, 3: 5.5})
# 上式右值等价于:
Vectors.sparse(4, [(1, 1.0), (3, 5.5)])
# 或:
Vectors.sparse(4, [1, 3], [1.0, 5.5])
```

不过目前来说, 我们的数据都是放在文件里的, 向量并不太常用. 这里只是做个示例, 说明向量也是一种Spark Source, 可以用来创建DataFrame, 借这个例子直观地理解一下DataFrame. Spark文档中的示例很多都是使用向量来创建DataFrame, 因此对理解文档也有帮助. 

```py
# 一列的DataFrame
data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
        (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
        (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
        (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]
df = spark.createDataFrame(data, ["features"])

# 两列的DataFrame
dataFrame = spark.createDataFrame([
    (0, Vectors.dense([1.0, 0.5, -1.0]),),
    (1, Vectors.dense([2.0, 1.0, 1.0]),),
    (2, Vectors.dense([4.0, 10.0, 2.0]),)
], ["id", "features"])
```

可以使用`dataframe.show()`展示出来:

```py
>>> df.show()
+--------------------+
|            features|
+--------------------+
|(4,[0,3],[1.0,-2.0])|
|   [4.0,5.0,0.0,3.0]|
|   [6.0,7.0,0.0,8.0]|
| (4,[0,3],[9.0,1.0])|
+--------------------+
```

```py
>>> dataFrame.show()
+---+--------------+
| id|      features|
+---+--------------+
|  0|[1.0,0.5,-1.0]|
|  1| [2.0,1.0,1.0]|
|  2|[4.0,10.0,2.0]|
+---+--------------+
```

可以看到, DataFrame的逻辑结构其实跟数据库的Table很相似.

### 问题 & 解决

注意`createDataFrame()`的传入参数有点奇怪, 下面这段代码会报错:

```py
dataframe = spark.createDataFrame([Vectors.dense([1.0, 2.0]), Vectors.dense([3.3, 7.7])], ['features'])
```

```py
TypeError: not supported type: <class 'numpy.ndarray'>
```

改成这样就不会了:

```py
# 把向量扩起来, 后面再加个逗号(必须加逗号, 不然也会报错)
dataframe = spark.createDataFrame([(Vectors.dense([1.0, 2.0]),),(Vectors.dense([3.3, 7.7]),)], ['features'])
```

## csv文件创建DataFrame

假设读取这样一个csv文件`example.csv`:

```csv
1,2,3,4
0.1,4.3,90.5,7
aaa,b,cc,f
```

```py
>>> dataframe = spark.read.csv('example.csv')
>>> dataframe.show()
+---+---+----+---+
|_c0|_c1| _c2|_c3|
+---+---+----+---+
|  1|  2|   3|  4|
|0.1|4.3|90.5|  7|
|aaa|  b|  cc|  f|
+---+---+----+---+
```

还可以将第一行视为header.

```
>>> dataframe = spark.read.csv('example.csv', header = True)
+---+---+----+---+
|  1|  2|   3|  4|
+---+---+----+---+
|0.1|4.3|90.5|  7|
|aaa|  b|  cc|  f|
+---+---+----+---+
```

# 回到机器学习

现在, 我们来写一个最简单的KMeans聚类算法. 文档可以参考[Clustering K-means](https://spark.apache.org/docs/latest/ml-clustering.html#k-means).

这个简单的机器学习算法使用kddcup1999的数据集, 因为仅仅是用来演示, 为了处理速度快一些, 我将这个数据集又缩小了一些, 现在约有20000多行, 并且, 没有标签.

首先, 创建一个spark对象.

```py
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```

现在可以通过csv文件创建一个DataFrame对象了.

```py
df = spark.read.csv('nolabel_rm_num_5_precent_kddcup.data')
# 这等价于下面这一句
df = spark.read.format('csv').load('nolabel_rm_num_5_precent_kddcup.data')
```

用`df.show(5)`把它打印出来看看:

```
+---+---+----+---+---+---+---+---+---+---+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
|_c0|_c1| _c2|_c3|_c4|_c5|_c6|_c7|_c8|_c9|_c10|_c11|_c12|_c13|_c14|_c15|_c16|_c17|_c18|_c19|_c20|_c21|_c22|_c23|_c24|_c25|_c26|_c27|_c28|_c29|_c30|_c31|_c32|_c33|_c34|_c35|_c36|_c37|
+---+---+----+---+---+---+---+---+---+---+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
|  0|181|5450|  0|  0|  0|  0|  0|  1|  0|   0|   0|   0|   0|   0|   0|   0|   0|   0|   8|   8|0.00|0.00|0.00|0.00|1.00|0.00|0.00|   9|   9|1.00|0.00|0.11|0.00|0.00|0.00|0.00|0.00|
|  0|239| 486|  0|  0|  0|  0|  0|  1|  0|   0|   0|   0|   0|   0|   0|   0|   0|   0|   8|   8|0.00|0.00|0.00|0.00|1.00|0.00|0.00|  19|  19|1.00|0.00|0.05|0.00|0.00|0.00|0.00|0.00|
|  0|235|1337|  0|  0|  0|  0|  0|  1|  0|   0|   0|   0|   0|   0|   0|   0|   0|   0|   8|   8|0.00|0.00|0.00|0.00|1.00|0.00|0.00|  29|  29|1.00|0.00|0.03|0.00|0.00|0.00|0.00|0.00|
|  0|256|1169|  0|  0|  0|  0|  0|  1|  0|   0|   0|   0|   0|   0|   0|   0|   0|   0|   4|   4|0.00|0.00|0.00|0.00|1.00|0.00|0.00|   4| 139|1.00|0.00|0.25|0.04|0.00|0.00|0.00|0.00|
|  0|241| 259|  0|  0|  0|  0|  0|  1|  0|   0|   0|   0|   0|   0|   0|   0|   0|   0|  12|  12|0.00|0.00|0.00|0.00|1.00|0.00|0.00|  94| 229|1.00|0.00|0.01|0.03|0.00|0.00|0.00|0.00|
+---+---+----+---+---+---+---+---+---+---+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
only showing top 5 rows
```

数据列比较多所以可能有点乱, 但可以看到, 列名是自动生成的.

可以看到所有项都是数字, 没有字符.

现在我们可以用这个数据集训练模型了.

```py
from pyspark.ml.clustering import KMeans
# 这里的k是集群个数, seed是什么?
kmeans = KMeans(k = 10, seed = 1)
model = kmeans.fit(df)
```.
pyspark.sql.utils.IllegalArgumentException: 'Field "features" does not exist.'
```

报错了! 核心错误就是这句: `'Field "features" does not exist.'`

我是按照官网上的教程写的, 只是数据集不一样. 官网上的源码是:

```py
dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")
```

而他用的数据读入内存后是这样的:

```
+-----+--------------------+
|label|            features|
+-----+--------------------+
|  0.0|           (3,[],[])|
|  1.0|(3,[0,1,2],[0.1,0...|
|  2.0|(3,[0,1,2],[0.2,0...|
|  3.0|(3,[0,1,2],[9.0,9...|
|  4.0|(3,[0,1,2],[9.1,9...|
|  5.0|(3,[0,1,2],[9.2,9...|
+-----+--------------------+
```

所以上面的错误是因为`fit()`函数(在不指定的情况下)默认读取`features`列作为特征向量, 但是我们的数据集都是`_c1`这样的名字, 导致`fit()`函数找不到训练对象.

因此我们要将我们的df弄成类似上面这个样子, 就需要将所有的列合并成一个features列.

```py
from pyspark.ml.feature import VectorAssembler
# df.columns返回一个所有列名组成的列表
vecAssembler = VectorAssembler(inputCols = df.columns, outputCol = 'features')
df = vecAssembler.transform(df)
```

运行一下改好的程序, 会发现仍然报错:

```
  File "SimpleKMeans.py", line 22, in <module>
    df = vecAssembler.transform(df)
pyspark.sql.utils.IllegalArgumentException: 'Data type StringType is not supported.'
```

原因是我们的df中的数据没被自动认做Double类型, 而是String类型. 于是需要先做类型转换:

```py
import pyspark.sql.functions as sqlf
cast = [sqlf.col(x).cast('Double').alias(x) for x in vecAssembler.getInputCols()]
df = df.select(*cast)
```

再运行程序就没毛病了. 现在可以打印一些东西出来, 比如:

```py
# 计算KMeans聚类效果, 值越低效果越好
wssse = model.computeCost(df)
```

```py
centers = model.clusterCenters()
for center in centers:
    print(center)
```

# 源代码

1. `/PysparkMLQuickStart/VectorAndDataFrame.py`
2. `/PysparkMLQuickStart/SimpleKMeans.py`.

# 参考

spark官方文档：

[Machine Learning Library (MLlib) Guide](https://spark.apache.org/docs/latest/ml-guide.html)

[Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
