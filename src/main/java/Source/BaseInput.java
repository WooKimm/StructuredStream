package Source;

import base.Base;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import parser.CreateTableParser;
//输入源的超类
public interface BaseInput extends Base {
    /*无论是什么类型的输入，创建一个Dstream以供后者使用*/
    Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config, int id);//id是为了用来确定是第几个源表

    //是对分隔符和isProcess的判断
    void beforeInput();


    /*转为schame的数据，再加上window之后返回*/
    void afterInput();

    String getName();
}

