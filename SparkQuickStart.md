这篇文章是spark的基本概念. 最好是配好了环境之后再来读这篇.

[spark环境配置](SparkBuild.md)

# Spark & Pyspark

[official site](http://spark.apache.org/)

这里简单介绍一下spark、pyspark。

> Apache Spark is a fast and general-purpose cluster computing system. 

在实际应用中，一般有多台服务器，并需要处理大量数据，需要一个工具来帮助我们在多台机器上处理数据。因此我们选择借助spark框架。

类似的框架还有Hadoop, 不过据说spark性能比较好.

pyspark可以有两个含义：一是指spark为python语言准备的交互式shell，也是spark面向python语言的API。

例如, 你可以用`pyspark`命令运行交互式命令行, 也可以将语句`import pyspark`写入Python程序.

## pyspark交互式shell

当配置好spark环境后，运行：`pyspark`可以进入spark的python交互式命令行。

现在你可以先把这段代码复制到shell中:

```py
from pyspark.ml.linalg import Vectors
data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),(Vectors.dense([4.0, 5.0, 0.0, 3.0]),)]
dataframe = spark.createDataFrame(data, ["features"])
dataframe.show()
```

我们先不解释这段代码具体什么意思--这在QuickStart教程中会解释. 

现在我们只看一个东西--第三行中的`spark`变量. 我们之前没有声明过这个变量, 现在却直接使用了, 这是不可能的. 于是, 这个spark对象就是Pyspark Shell为我们预处理的部分工作了. Pyspark shell在运行时会:

1. 创建一个SparkContext对象, 名叫`sc`, 这个对象可以用来创建RDD对象.
2. 创建一个SparkSession对象, 名叫`spark`, 这个对象可以用来创建DataFrame对象.

在spark官方文档中出现的, 上文没有定义过却使用了的变量`sc`, `spark`就是它俩了. 所以如果你直接复制官网上的源代码到文件中(而不是pyspark shell), 运行时会报错. 这是因为不在交互式shell中时, 你需要自己创建这两个变量.

在这里只需要先知道这点就行了: 如果你使用交互式命令行spark, 那么可以直接使用`spark`和`sc`对象. 否则你需要自己创建他们.

# 运行应用

## spark submit

使用`spark-submit`脚本来提交应用. 

为什么要使用这个脚本而不是直接使用`python`命令?

## local模式(单机)

如果你没有spark应用, 你可以使用spark自带的示例程序. Python语言的示例程序在`spark目录/examples/src/main/python`目录中.

如果你要运行spark示例, 应该在spark目录中运行(也就是说后面一大长串你都要输入), 否则有些需要读取文件的程序会找不到文件.

```sh
cd "$SPARK_HOME"
spark-submit  examples/src/main/python/kmeans.py
```

或者你写了一个独立应用，然后可以使用spark脚本来提交：

```sh
spark-submit spark-app.py
# 或者
spark-submit spark-app.py --master local
```

如果你没有更改spark的其他配置，这么这两条命令是等价的，都是在本地模式(单机)运行一个spark应用。

你提交的这个应用可以是一个`.py`文件，也可以是一个`.zip`或`.egg`格式的文件包。

如果你提交一个`.py`文件，就应该保证这个文件的所有依赖项都存在在worker节点上。例如你提交一个`.py`文件，这个文件包含了模块`numpy`，但worker节点没有安装`numpy`这个包，那么就会报错。因此如果你不能保证worker节点上的依赖的完整性，就应当把所有依赖打包成`.zip`或`.egg`提交给spark。

## 集群模式运行应用

在这里只说明standalone集群模式(其他两种我没有搭建过, 不会)。这个模式中，运行应用只需要操作master节点，master节点会操作其他slave节点。

在集群上运行应用大概需要两步(当然, 你可能需要先ssh到master节点): 

### 启动standalone集群

由于spark有三种集群模式，而每一种模式的启动方法是不同的，因此需要先分别启动集群，才能将应用提交到集群上运行。(standalone集群的启动貌似只是master与各个slave节点建立了ssh连接)

```sh
start-all.sh
```

`start-all.sh`运行后，集群会一直处于启动状态，下一次不再需要运行`start-all.sh`命令，直到运行`stop-all.sh`停止集群。

### 提交应用

对于每一种集群模式，提交应用给spark的接口都是相同的`spark-submit`命令，只有在指定`--master`参数的时候不同。

```
spark-submit [spark-application] --master spark://127.0.0.1:8888
```

这里`--master`后面的`spark://`表明使用的是spark的standalone集群模式。端口可以随意指定。

## 例子

在master节点上运行命令:

```sh
# 运行spark自带的示例
# 假如我的spark安装在这里了:
cd ~/spark-2.2.0-bin-hadoop2.7
# 现在运行一个spark示例(spark自带的, 不是我写的)
spark-submit examples/src/main/python/kmeans.py --master spark://127.0.0.1:8888
# 如果不加参数，应用默认只会运行在master机器上。
```

## Debug

如果上述任何一个环节出现了错误，说明spark环境没有配置好，见spark环境配置.

如果照着环境配置说明的做了, 也没有解决, 可以google一下, 或者联系我...
