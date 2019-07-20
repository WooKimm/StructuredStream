package sparkcl;

import Target.UniteOutput;
import Util.BaseZookeeper;
import Util.SparkUtil;
import Util.TestForeachWriter;
import Util.VideoWriter;
import com.amd.aparapi.Range;
import com.aparapi.Kernel;
import javafx.scene.chart.PieChart;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import parser.CreateTableParser;
import parser.InsertSqlParser;
import parser.SqlParser;
import parser.SqlTree;
import redis.clients.jedis.Tuple;
import scala.Tuple1;
import scala.Tuple2;

import java.util.*;

public class StreamTest {
    static SparkSession spark = null;
    public static void main(String[] args) throws Exception {

        //第一阶段
        BaseZookeeper zookeeper = new BaseZookeeper();
        zookeeper.connectZookeeper("127.0.0.1:2181");
        String sql = BaseZookeeper.getSqlFromSource("csv");
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
        Map<String,Dataset<Row>> tableList = Util.SparkUtil.getTableList(spark,SqlParser.sqlTree);



        //第五阶段

        SparkUtil.createStreamingQuery(spark,sqlTree,tableList);
        spark.streams().awaitAnyTermination();

    }




}
