# StructuredStream
尝试复现关于支持滑动窗口的SparkSQL查询接口 扩展与实现论文中的代码与测试

## 关于ZooKeeper
在resource文件夹下的testSQLFile内容为测试sql文件。
打开zookeeper服务端后，将SparkInSql中已注释的
```java
Stat stat= zookeeper.setData(...);
```
语句解注释运行即可将测试文件内容添加到名为sqlTest的节点上。

## 对于Kafka源的输入
首先安装Kafka环境：[官网安装地址](http://kafka.apache.org/downloads.html)，选择binary dowload
之后按照[官方文档中](https://kafka.apache.org/documentation/#quickstart)的方法分别启动zookeeper、kafka服务器，再创建topic：
```shell
>bin/zookeeper-server-start.bat config/zookeeper.properties
>bin/kafka-server-start.bat config/server.properties
>bin/kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test
```

发送消息给test：
```shell
>bin/kafka-console-producer.bat --broker-list localhost:9092 --topic test
```
（windows下需运行windows文件夹下对应的bat文件）
