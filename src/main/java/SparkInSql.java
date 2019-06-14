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

        Stat stat= zookeeper.setData("/sqlTest","create env spark(\n" +
                "    spark.default.parallelism='2',\n" +
                "    spark.sql.shuffle.partitions='2'\n" +
                ")WITH(\n" +
                "    appname='WooTest'\n" +
                ");" +
                "CREATE TABLE InputTable(\n" +
                "    number Int\n" +
                ")WITH(\n" +
                "    type='socket',\n" +
                "    host='localhost',\n" +
                "    processwindow='10 seconds,5 seconds',\n" +
                "    port='9998'\n" +
                ");\n" +
                "\n" +
                "CREATE SINK OutputTable(\n" +
                ")WITH(\n" +
                "    type='console',\n" +
                "    outputmode='update'\n" +
                ");\n" +
                "\n" +
                "insert into OutputTable select processwindow,number,count(*) from InputTable group by processwindow,number;\n");
//        System.out.println(children);
        String testData = zookeeper.getData("/sqlTest");
        parseSql(testData);
        System.out.println(testData);




//        while (true){
//            if(!SparkUtil.isAdd){
//                //TODO 传入的参数进行解析
//                SparkSession spark = SparkUtil.getSparkSession();
//                spark.sparkContext().setLogLevel("WARN");
//
//                DynamicChangeUtil.CuratorWatcher(getZkclient(),"sqlstream/sql");
//                SparkUtil.parseProcessAndSink(spark, SqlParser.sqlTree);
//
//                spark.streams().addListener(new StreamingQueryListener() {
//                    @Override
//                    public void onQueryStarted(QueryStartedEvent queryStarted) {
//                        System.out.println("Query started: " + queryStarted.id());
//                    }
//                    @Override
//                    public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
//                        System.out.println("Query terminated: " + queryTerminated.id());
//                    }
//                    @Override
//                    public void onQueryProgress(QueryProgressEvent queryProgress) {
//
//                        System.out.println("Query made progress: " + queryProgress.progress().id());
//                        int length = spark.sessionState().streamingQueryManager().active().length;
//                        System.out.println("active query length:"+length);
//
//                    }
//                });
//                SparkUtil.streamingQuery.awaitTermination();
//            }
//        }
    }
}
