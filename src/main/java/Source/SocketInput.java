package Source;

import Util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import parser.CreateTableParser;

import java.util.HashMap;
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
        //lineRow = GetSchemaDataSet(lineRow,);
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
}
