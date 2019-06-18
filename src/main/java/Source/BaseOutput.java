package Source;

import base.Base;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import parser.CreateTableParser;
import parser.InsertSqlParser;
import parser.SqlTree;

public interface BaseOutput extends Base {
    StreamingQuery process(SparkSession spark, Dataset<Row> dataset, CreateTableParser.SqlParserResult config, SqlTree sqlTree);

}

