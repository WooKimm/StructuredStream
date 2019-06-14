import Util.BaseZookeeper;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;

import static parser.SqlParser.parseSql;

public class SparkInSql {
    public static void main(String[] args) throws Exception {
        BaseZookeeper zookeeper = new BaseZookeeper();
        zookeeper.connectZookeeper("127.0.0.1:2181");
//        List<String> children = zookeeper.getChildren("/");

//        Stat stat= zookeeper.setData("/sqlTest","create env spark(\n" +
//                "    spark.default.parallelism='2',\n" +
//                "    spark.sql.shuffle.partitions='2'\n" +
//                ")WITH(\n" +
//                "    appname='WooTest'\n" +
//                ");" +
//                "CREATE TABLE InputTable(\n" +
//                "    number Int\n" +
//                ")WITH(\n" +
//                "    type='socket',\n" +
//                "    host='localhost',\n" +
//                "    processwindow='10 seconds,5 seconds',\n" +
//                "    port='9998'\n" +
//                ");\n" +
//                "\n" +
//                "CREATE SINK OutputTable(\n" +
//                ")WITH(\n" +
//                "    type='console',\n" +
//                "    outputmode='update'\n" +
//                ");\n" +
//                "\n" +
//                "insert into OutputTable select processwindow,number,count(*) from InputTable group by processwindow,number;\n");
////        System.out.println(children);
        String testData = zookeeper.getData("/sqlTest");
        parseSql(testData);
        System.out.println(testData);



    }
}
