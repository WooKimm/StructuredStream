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
    Boolean isTrigger = false;
    Boolean isOnce = false;
    Boolean isContinue = false;
    String triggerTime = "";
    Map<String, Object> consoleMap = null;

    @Override
    public StreamingQuery process(SparkSession spark, Map<String,Dataset<Row>> tablelist, CreateTableParser.SqlParserResult config, InsertSqlParser.SqlParseResult execSql) {
        consoleMap = config.getPropMap();
        checkConfig();

        String querySql = execSql.getQuerySql();

        for (String key: tablelist.keySet()) {
            Dataset<Row> sourceTable = tablelist.get(key);
            sourceTable.createOrReplaceTempView(key);
        }
        Dataset<Row> result = spark.sql(querySql);

        //生成query
        StreamingQuery query = null;
        if(isTrigger)
        {
            if(isContinue)
            {
                query = result.writeStream()
                        .outputMode(outputmode)
                        .format("console")
                        .trigger(Trigger.Continuous(triggerTime))
                        .option("truncate", false)
                        .start();
            }
            else
            {
                if(isOnce)
                {
                    query = result.writeStream()
                            .outputMode(outputmode)
                            .format("console")
                            .trigger(Trigger.Once())
                            .option("truncate", false)
                            .start();
                }
                else
                {
                    query = result.writeStream()
                            .outputMode(outputmode)
                            .format("console")
                            .trigger(Trigger.ProcessingTime(triggerTime))
                            .option("truncate", false)
                            .start();
                }
            }
        }
        else
        {
            System.out.println("触发器未配置，使用默认触发器");
            query = result.writeStream()
                    .outputMode(outputmode)
                    .format("console")
                    .option("truncate", false)
                    .start();
        }
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
        if(consoleMap.containsKey("trigger")||consoleMap.containsKey("continuetrigger"))
        {
            isTrigger = true;
            if(consoleMap.containsKey("trigger"))
            {
                String time = consoleMap.get("trigger").toString();
                if(time.equals("once"))
                {
                    isOnce = true;
                }
                else
                {
                    triggerTime = time;
                }
            }
            else
            {
                isContinue = true;
                triggerTime = consoleMap.get("continuetrigger").toString();
            }
        }
    }

    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        return null;
    }
}
