package Target;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import parser.CreateTableParser;
import parser.InsertSqlParser;
import parser.SqlTree;

import java.util.HashMap;
import java.util.Map;

public class ConsoleOutput implements BaseOutput {
    String outputmode = "complete";
    Map<String, Object> consoleMap = null;

    @Override
    public StreamingQuery process(SparkSession spark, Map<String,Dataset<Row>> tablelist, CreateTableParser.SqlParserResult config, SqlTree sqlTree) {
        InsertSqlParser.SqlParseResult execSql = sqlTree.getExecSql();
        consoleMap = config.getPropMap();
        checkConfig();

        String querySql = execSql.getQuerySql();

        for (String key: tablelist.keySet()) {
            Dataset<Row> sourceTable = tablelist.get(key);
            sourceTable.createOrReplaceTempView(key);
        }
        Dataset<Row> result = spark.sql(querySql);

        //生成query
        StreamingQuery query = result.writeStream()
                .outputMode(outputmode)
                .format("console")
                .trigger(Trigger.ProcessingTime("2 seconds"))//todo: 需要修改触发器生成方式
                .start();
        return query;
    }

    @Override
    public void checkConfig() {
        if(consoleMap.containsKey("outputmode"))
        {
            outputmode = consoleMap.get("outputmode").toString();
        }
        else
        {
            System.out.println("输出模式未配置，默认为完整模式");
        }
    }

    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        return null;
    }
}
