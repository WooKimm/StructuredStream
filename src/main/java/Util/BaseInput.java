package Util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import parser.CreateTableParser;

public interface BaseInput extends Base{
    /*无论是什么类型的输入，创建一个Dstream以供后者使用*/
    Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config);
    void beforeInput();
    /*输出后要做的事情，例如更新偏移量*/
    void afterInput();
    String getName();
}

