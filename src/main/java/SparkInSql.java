import Util.BaseZookeeper;
import Util.DataSender;
import Util.SparkUtil;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import parser.CreateTableParser;
import parser.InsertSqlParser;
import parser.SqlParser;
import parser.SqlTree;

import static parser.SqlParser.parseSql;

public class SparkInSql {

    static SparkSession spark = null;

    public static void main(String[] args) throws Exception {



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



//        DataSender dataSender = new DataSender("sender");
//        dataSender.start();//向9998端口发送1-100随机数

        //第一阶段
        BaseZookeeper zookeeper = new BaseZookeeper();
        zookeeper.connectZookeeper("127.0.0.1:2181");

        String testData = zookeeper.getData("/sqlTest");
        //第二阶段
        SqlParser.parseSql(testData);
        SqlTree sqlTree = SqlParser.sqlTree;

        //第三阶段
        SparkConf sparkConf = new SparkConf();
        Map<String, Object> preDealSparkEnvMap = sqlTree.getPreDealSparkEnvMap();//spark env配置加上
        preDealSparkEnvMap.forEach((key,value)->{
            sparkConf.set(key,value.toString());
        });
        spark = SparkSession
                .builder()
                .config(sparkConf)
                .appName(sqlTree.getAppInfo())
                .master("local[2]")
                .getOrCreate();



        spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent queryStarted) {
                System.out.println("Query started: " + queryStarted.id());
            }
            @Override
            public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
                System.out.println("Query terminated: " + queryTerminated.id());
            }
            @Override
            public void onQueryProgress(QueryProgressEvent queryProgress) {
                System.out.println("Query made progress: " + queryProgress.progress());
            }
        });


        //第四阶段
        Map<String,Dataset<Row>> tableList = SparkUtil.createDataFrame(spark,SqlParser.sqlTree);


        //第五阶段
        StreamingQuery streamingQuery = null;
        for (String key : tableList.keySet())
        {
            streamingQuery = SparkUtil.createStreamingQuery(spark,sqlTree,tableList.get(key));//只支持一个sourse table
        }

        streamingQuery.awaitTermination();

    }
}
