package Target;

import base.Base;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import parser.CreateTableParser;
import parser.InsertSqlParser;
import parser.SqlTree;

import java.util.Map;

public interface BaseOutput extends Base {

    StreamingQuery process(SparkSession spark, Map<String,Dataset<Row>> tablelist, CreateTableParser.SqlParserResult config, InsertSqlParser.SqlParseResult execSql);


}

