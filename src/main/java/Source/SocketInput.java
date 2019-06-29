package Source;

import Util.ColumnType;
import Util.DatasetUtil;
import Util.SparkUtil;
import org.aopalliance.reflect.Class;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import parser.CreateTableParser;
import parser.SqlTree;
import scala.Product;
import scala.Tuple2;
import scala.Tuple3;

import java.sql.Timestamp;
import java.util.*;

public class SocketInput implements BaseInput{

    Map<String, Object> socketMap = null;
    CreateTableParser.SqlParserResult config = null;
    Boolean isProcess = false;
    Dataset<Row> result = null;
    int id;

    public Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config, int id) {
        socketMap = config.getPropMap();
        this.config = config;
        this.id = id;
        checkConfig();
        beforeInput();
        //获取prepare后具有field的dataset
        result = prepare(spark);
        //获取具有schema和window的dataset
        afterInput();
        return result;
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

    //提取窗口信息
    @Override
    public void beforeInput() {
        String delimiter = null;
        try {
            delimiter = socketMap.get("delimiter").toString();
            //判断window类型
        } catch (Exception e) {
            System.out.println("分隔符未配置，默认为逗号");
        }

        if (delimiter == null) {
            socketMap.put("delimiter", ",");
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

    //注册生成最初始的dataframe，只有一列，列名value
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

    @Override
    public void afterInput() {
        String delimiter = socketMap.get("delimiter").toString();
        SqlTree.delimiters.add(delimiter);
        List<ColumnType> column = new ArrayList<>();
        SqlTree.columnLists.add(column);
        result = DatasetUtil.getSchemaDataSet(result, config.getFieldsInfoStr(), isProcess, config.getPropMap(), id);
    }

    public String getName() {
        return "name";
    }




}
