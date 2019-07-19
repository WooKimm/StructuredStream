import Util.BaseZookeeper;
import Util.DataSender;
import Util.SparkUtil;
import base.Base;
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

import static Util.SparkUtil.createStreamingQuery;
import static parser.SqlParser.parseSql;

public class SparkInSql {

    static SparkSession spark = null;

    public static void main(String[] args) throws Exception {


        //第一阶段
        BaseZookeeper zookeeper = new BaseZookeeper();
        zookeeper.connectZookeeper("127.0.0.1:2181");
        String sql = BaseZookeeper.getSqlFromSource("socket");
        zookeeper.setData("/sqlTest", sql);



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
        Map<String,Dataset<Row>> tableList = SparkUtil.getTableList(spark,SqlParser.sqlTree);

        //第五阶段

        SparkUtil.createStreamingQuery(spark,sqlTree,tableList);
        spark.streams().awaitAnyTermination();

    }
}
