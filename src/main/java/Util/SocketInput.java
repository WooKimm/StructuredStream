package Util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import parser.CreateTableParser;

import java.util.Map;

public class SocketInput implements BaseInput {
    @Override
    public Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config) {
        Map propMap = config.getPropMap();
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", propMap.get("host").toString())
                .option("port", propMap.get("port").toString())
                .load();
        return lines;
    }

    @Override
    public void beforeInput() {

    }

    @Override
    public void afterInput() {

    }

    @Override
    public String getName() {
        return null;
    }
}
