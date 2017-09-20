Apache Spark是一个通用大数据处理框架. 这篇文章说明如何配置Spark的Python编程环境. 

如果你已经会配置Spark环境了, 你可以试试我的自动配置脚本: [server-build](https://github.com/syxbyi97/server-build)

# spark本地模式

spark本地模式，就是在单机上运行的spark。你可以在自己的电脑上配置本地模式的spark来编辑和调试spark应用。

## 安装java

spark运行在java虚拟机上，需要先安装java。在Ubuntu系统中，运行命令：

```sh
sudo apt-get install default-jdk
```

## 下载spark

在[官网](https://spark.apache.org/downloads.html)下载预编译hadoop的spark安装包。

也可以不使用命令，在图形界面中解压安装包，复制到你想要的目录。

如果你是在自己的电脑上配置开发环境，那么我建议将spark配置到你的家目录下，即将spark解压到家目录的子目录下，并将环境变量添加到`~/.bashrc`中。否则，有可能面临一些权限问题。

## 环境变量

添加环境变量可以让你从任何目录运行spark脚本和命令，这意味着你可以只输入命令名称——而不是完整路径——来使用一个spark命令。添加环境变量也使python脚本可以运行`import pyspark`语句。

## 例子

例如，我将spark文件包下载到了`~/Download`目录下，包名是spark-2.2.0-bin-hadoop2.7.tgz，并解压：

如果你对下面这些命令没有把握，我推荐你用图形界面做同样的事情。

```sh
cd ~/Download
tar -zvxf spark-2.2.0-bin-hadoop2.7.tgz
```

解压后的目录名是spark-2.2.0-bin-hadoop2.7，我把它移动到`~/spark/`下：

```sh
mkdir ~/spark
mv spark-2.2.0-bin-hadoop2.7 ~/spark/
```

然后我在我的`.bashrc`文件中添加环境变量：

```sh
# SPARK_HOME的值应该根据你的实际安装地点而修改
export SPARK_HOME="$HOME/spark/spark-2.2.0-bin-hadoop2.7"
export PATH="$SPARK_HOME/sbin:$SPARK_HOME/bin:$PATH"
# python导入包时查找的路径。把pyspark模块加入路径中，就可以import pyspark
export PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
# py4j-0.10.4-src.zip这个包的名称可能会因spark版本不同而不同，请确认你安装的版本中的名称
export PYTHONPATH="$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH"
```

重新启动一个terminal，或者用`source ~/.bashrc`来使环境变量生效。

## 测试环境

试运行spark for python的交互式脚本：

```sh
pyspark
```

尝试在python程序中`import pyspark`：

```sh
python
>>> import pyspark
>>> # 没有报错就是成功了
```

# spark集群环境

spark集群模式一共有三种：standalone，Mesos，YARN。

# TODO

具体都是什么、有什么区别，我也不知道。

这里我们选择的集群是spark自带的standalone模式，这个模式最容易搭建。

## 安装

对于每一台机器，安装java、spark和sshserver。

java的安装和单机模式是一样的。

### spark安装

下载spark安装包，解压到目标目录就可以了。

对于每一台机器，standalone模式要求spark的目录都相同。本项目中，spark统一安装在`/usr/lib/spark/spark-2.2.0-bin-hadoop2.7`。

### sshserver安装

master需要通过ssh来与slaves通信，因此需要ssh服务。

```sh
sudo apt-get install openssh-server
```

## 配置

### 环境变量

和单机模式一样，需要配置环境变量来更方便地运行脚本。

本项目中，集群环境使用的配置文件是`/etc/profile`：

```sh
# 跟本地模式相比，变了的只有SPARK_HOME的路径
export SPARK_HOME="/usr/lib/spark/spark-2.2.0-bin-hadoop2.7"
export PATH="$SPARK_HOME/sbin:$SPARK_HOME/bin:$PATH"
export PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
export PYTHONPATH="$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH"
```

重新启动终端，或者运行`source /etc/profile`载入配置文件。

### hosts文件

使用集群的spark需要知道各个节点的ip地址，才能建立ssh连接。为了便捷、复用和清晰，我们将ip地址写入hosts文件中，这样就可以使用名称来指定服务器，而不是每次都直接写ip地址。

修改hosts文件`/etc/hosts`来指定ip地址：

```sh
# 前一项是ip地址，后一项是对应机器的hostname
192.168.31.200 master
192.168.31.203 slave01
192.168.31.205 slave02
192.168.31.204 slave03
```

为了让hosts文件能长期使用，ip地址应尽量保持不变。本项目中服务器的ip地址是用交换机配置的，根据MAC地址绑定的内网ip(因此不要更换每台服务器的网线插口，这会导致ip变化)。

### ssh免密

spark的master节点通过ssh与slaves互传信息和命令，不可能每一次ssh连接都要求密码(那个太烦人了)，因此需要配置ssh免密登录。

首先，在master节点上，创建一个ssh密钥：

```sh
ssh-keygen
```

一路enter就可以创建成功。

将master的ssh公钥通过`scp`命令拷贝给每一个slave(如果你有3台slave机器，就要拷贝三次)：

```sh
scp ~/.ssh/id_rsa.pub [hostname]@[ip]:[path]
```

例如，对于我们的机器，命令是这样的：

```sh
# 这是第一台slave，接下来要把ip换成第二台slave的继续拷贝
scp ~/.ssh/id_rsa.pub nkamg@192.168.31.203:/home/nkamg/
```

ssh连接到每一个slave节点上，并执行命令——把刚才拷贝过去的公钥写入文件`~/.ssh/authorized_keys`：

```sh
ssh nkamg@192.168.31.203 "mkdir ~/.ssh/ && cat ~/id_rsa.pub >> ~/.ssh/authorized_keys"
```

现在试试ssh到刚才配置的那台服务器上，试试ssh免密登录：

```sh
ssh nkamg@192.168.31.203
```

### spark配置文件

将模板文件复制一份：

```sh
cd $SPARK_HOME/conf
cp slaves{.template,}
cp spark-env.sh{.template,}
```

配置slaves文件，内容就是刚才写入hosts文件中的slaves的hostname，每行一个(只需要写slave机器就可以，不用写master)。运行spark脚本`start-slaves.sh`时，会读取这里的slaves并依次建立ssh连接。

```
slave01
slave02
slave03
```

# TODO

将运行配置写入`spark-env.sh`文件：

```
export SPARK_MASTER_IP=127.0.0.1
export SPARK_WORKER_MEMORY=3g
```

# 参考

试运行Spark和Spark编程基础: [SparkQuickStart](SparkQuickStart.md)

[SSH免密登录原理及配置](https://my.oschina.net/binxin/blog/651565)

[Spark Standalone Mode](http://colobu.com/2014/12/09/spark-standalone-mode/)

[Spark 应用提交指南](http://colobu.com/2014/12/09/spark-submitting-applications/)
