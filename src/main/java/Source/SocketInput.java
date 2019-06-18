package Source;

import Util.SparkUtil;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import parser.CreateTableParser;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SocketInput implements BaseInput{

    Map<String, Object> socketMap = null;
    String windowType = "";
    Boolean isProcess = true;

    public Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config) {
        socketMap = config.getPropMap();
        checkConfig();
        beforeInput();
        //获取prepare后具有field的dataset
        Dataset<Row> lineRow = prepare(spark);
        //获取具有schema的dataset
        lineRow = GetSchemaDataSet(lineRow,config);
        //获取window类型并处理后的dataset
//        Dataset<Row> lineRowWithWindow = GetWindowDataset(lineRow,socketMap);
        return lineRow;
    }

    //提取窗口信息
    @Override
    public void beforeInput() {
        String delimiter = null;
        String proWindow = null;
        String eventWindow = null;
        try {
            delimiter = socketMap.get("delimiter").toString();
            //判断window类型
        } catch (Exception e) {
            System.out.println("分隔符未配置，默认为逗号");
        }
        try {
            proWindow = socketMap.get("processwindow").toString();
            eventWindow = socketMap.get("eventwindow").toString();
            //判断window类型
        } catch (Exception e) {

        }
        if (delimiter == null) {
            socketMap.put("delimiter", ",");
        }
        if (proWindow != null) {
            windowType = "process " + proWindow;
        }
        if (eventWindow != null) {
            windowType = "event " + eventWindow;
        }
        if (socketMap.containsKey("processwindow")) {
            isProcess = true;
            socketMap.put("isProcess", true);
        }
        if (socketMap.containsKey("eventwindow")) {
            isProcess = false;
            socketMap.put("isProcess", false);
        }
    }

    @Override
    public void afterInput() {

    }

    //检查config
    @Override
    public void checkConfig() {
        Boolean isValid = socketMap.containsKey("host") &&
                !socketMap.get("host").toString().trim().isEmpty() &&
                socketMap.containsKey("port") &&
                !socketMap.get("host").toString().trim().isEmpty();
        if (!isValid) {
            throw new RuntimeException("host 或 port不能为空");
        }
    }

    //将生成的datastream转化为具有field的形式
    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        Dataset<Row> lines = null;
        if (isProcess) {
            lines = spark
                    .readStream()
                    .format("socket")
                    .option("host",socketMap.get("host").toString())
                    .option("port",socketMap.get("port").toString())
                    .option("includeTimestamp", true)
                    .load();
        } else {
            lines = spark
                    .readStream()
                    .format("socket")
                    .option("host",socketMap.get("host").toString())
                    .option("port",socketMap.get("port").toString())
                    .load();
        }
        return lines;
    }

    public String getName() {
        return "name";
    }


    //将生成的datastream转化为具有schema的形式
    public static Dataset<Row> GetSchemaDataSet(Dataset<Row> lineRow,CreateTableParser.SqlParserResult config){
        Dataset<Row> schemaRow = lineRow
                .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
                            List<Tuple2<String, Timestamp>> result = new ArrayList<>();
                            for (String word : t._1.split(",")) {
                                result.add(new Tuple2<>(word, t._2));
                            }
                            return result.iterator();
                        },
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
                ).toDF("number", "timestamp");
        return schemaRow;
    }

    //对窗口进行解析
//    public static Dataset<Row> GetWindowDataset(Dataset<Row> lineRow,Map<String,Object> propMap){
//        Dataset<Row> waterMarkData = null;
//        String eventfield;
//        Boolean isProcess = (Boolean) propMap.get("isProcess");
//        if (propMap.containsKey("watermark")) {
//            if (!propMap.containsKey("eventfield") && !isProcess) {
//                throw new RuntimeException("缺少eventfield");
//            }
//            if (isProcess) {
//                waterMarkData = lineRow.withWatermark("timestamp", propMap.get("watermark").toString());
//            } else {
//                eventfield = propMap.get("eventfield").toString();
//                waterMarkData = lineRow.withWatermark(eventfield, propMap.get("watermark").toString());
//            }
//        } else {
//            waterMarkData = lineRow;
//        }
//        //判断窗口类型
//
//        try {
//            //判断是process类型
//            proWindow = proMap.get("processwindow").toString();
//            //判断window类型
//        } catch (Exception e) {
//        }
//        try {
//            //判断是event类型
//            eventWindow = proMap.get("eventwindow").toString();
//            //判断window类型
//        } catch (Exception e) {
//        }
//        WindowType windowType =
//        if (windowType!=null) {
//            String[] splitTime = windowType.getWindow().split(",");
//            String windowDuration;
//            String slideDuration;
//            if (splitTime.length == 1) {
//                windowDuration = splitTime[0].trim();
//                slideDuration = splitTime[0].trim();
//            } else if (splitTime.length == 2) {
//                windowDuration = splitTime[0].trim();
//                slideDuration = splitTime[1].trim();
//            } else {
//                throw new RuntimeException("window的配置的长度好像有点问题呦");
//            }
//            switch (windowType.getType()) {
//                case "event":
//                    windowData = waterMarkData.withColumn("eventwindow", functions.window(waterMarkData.col(timeField), windowDuration, slideDuration));
//                    break;
//                case "process":
//                    windowData = waterMarkData.withColumn("processwindow", functions.window(waterMarkData.col("timestamp"), windowDuration, slideDuration));
//                    break;
//                default:
//                    windowData = waterMarkData;
//                    break;
//            }
//        } else {
//            windowData = transDataSet;
//        }
//
//        return windowData;
//    }


}
