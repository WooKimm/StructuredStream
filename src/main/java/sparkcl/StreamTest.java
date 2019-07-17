//package sparkcl;
//
//import Target.UniteOutput;
//import Util.BaseZookeeper;
//import Util.TestForeachWriter;
//import Util.VideoWriter;
//import com.amd.aparapi.Range;
//import com.aparapi.Kernel;
//import javafx.scene.chart.PieChart;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.streaming.StreamingQuery;
//import org.apache.spark.sql.streaming.StreamingQueryException;
//import org.apache.spark.sql.streaming.StreamingQueryListener;
//import org.apache.spark.sql.streaming.Trigger;
//import parser.CreateTableParser;
//import parser.InsertSqlParser;
//import parser.SqlParser;
//import parser.SqlTree;
//import redis.clients.jedis.Tuple;
//import scala.Tuple1;
//import scala.Tuple2;
//
//import java.util.*;
//
//public class StreamTest {
//    public static void main(String[] args) throws Exception {
//
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("JavaStructuredNetworkWordCount")
//                .getOrCreate();
//
//        Dataset<Row> lines = spark
//                .readStream()
//                .option("sep", ";")
//                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
//                // Specify schema of the csv files  是个文件目录
//                .csv("filepath/csv");
//
//        // Split the lines into words
//        Dataset<String> words = lines.as(Encoders.STRING()).flatMap(
//                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
//                Encoders.STRING());
//        JavaRDD<String> mywords = words.toJavaRDD();
//
////
////        // Generate running word count
////        Dataset<Row> wordCounts = words.groupBy("value").count();
////
////        // Start running the query that prints the running counts to the console
////        StreamingQuery query = wordCounts.writeStream()
////                .outputMode("complete")
////                .format("console")
////                .start();
////
////        query.awaitTermination();
//
//
//        SparkKernel<Tuple1<String>,Tuple2<String,Integer>> kernel = new SparkKernel<Tuple1<String>,Tuple2<String, Integer>>() {
//            // data
//            Tuple2<String,Integer> result;
//            Tuple1<String> theinput;
//            @Override
//            public void mapParameters(Tuple1<String> input) {
//
//                setExecutionMode(EXECUTION_MODE.JTP);
//                theinput = input;
//                // setExecutionMode(EXECUTION_MODE.JTP); // choose run mode i.e run on JTP/CPU/Accelerator etc.
//            }
//
//            @Override
//            public void run() {
//                result = theinput.groupBy("value").count();
//            }
//
//            @Override
//            public Tuple2<String,Integer> mapReturnValue(Tuple1<String> input) {
//                return result;
//            }
//        };
//
//        Dataset<Row> wordCounts = SparkUtil.genSparkCL().mapCL(kernel);
//
//        StreamingQuery query = wordCounts.writeStream()
//                .outputMode("complete")
//                .format("console")
//                .start();
//        query.awaitTermination();
//
//    }
//
//
//
//
//}
