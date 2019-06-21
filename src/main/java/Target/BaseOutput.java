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
<<<<<<< HEAD:src/main/java/Source/BaseOutput.java
    StreamingQuery process(SparkSession spark, Dataset<Row> dataset, CreateTableParser.SqlParserResult config, SqlTree sqlTree);
    StreamingQuery process2(SparkSession spark, Dataset<Row> dataset, CreateTableParser.SqlParserResult config);
=======
    StreamingQuery process(SparkSession spark, Map<String,Dataset<Row>> tablelist, CreateTableParser.SqlParserResult config, SqlTree sqlTree);
>>>>>>> 88c5745e408294a6833dd2f7ef2197e7f8d5203a:src/main/java/Target/BaseOutput.java

}

