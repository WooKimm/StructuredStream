# StructuredStream
尝试复现关于支持滑动窗口的SparkSQL查询接口 扩展与实现论文中的代码与测试

## 关于ZooKeeper
在resource文件夹下的testSQLFile内容为测试sql文件。
打开zookeeper服务端后，将SparkInSql中已注释的
```java
Stat stat= zookeeper.setData(...);
```
语句解注释运行即可将测试文件内容添加到名为sqlTest的节点上。
