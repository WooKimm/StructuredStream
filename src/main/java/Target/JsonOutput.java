package Target;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import parser.CreateTableParser;
import parser.InsertSqlParser;

import java.util.HashMap;
import java.util.Map;

public class JsonOutput implements BaseOutput{
    Boolean isTrigger = false;
    Boolean isOnce = false;
    Boolean isContinue = false;
    String triggerTime = "";
    Map<String, Object> jsonMap = null;

    @Override
    public StreamingQuery process(SparkSession spark, Map<String,Dataset<Row>> tablelist, CreateTableParser.SqlParserResult config, InsertSqlParser.SqlParseResult execSql) {
        jsonMap = config.getPropMap();
        checkConfig();

        String querySql = execSql.getQuerySql();

        for (String key: tablelist.keySet()) {
            Dataset<Row> sourceTable = tablelist.get(key);
            sourceTable.createOrReplaceTempView(key);
        }
        Dataset<Row> result = spark.sql(querySql);


        Map<String, String> options = new HashMap<>();
        options.put("path", jsonMap.get("path").toString());
        String num = String.valueOf((int)(1+Math.random()*(100-1+1)));
        //生成query
        StreamingQuery query = null;
        if(isTrigger)
        {
            if(isContinue)
            {
                query = result.writeStream()
                        .format("json")
                        .options(options)
                        .option("checkpointLocation", "path/to/HDFS2/dir"+num)
                        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                        .trigger(Trigger.Continuous(triggerTime))
                        .start();
            }
            else
            {
                if(isOnce)
                {
                    query = result.writeStream()
                            .format("json")
                            .options(options)
                            .option("checkpointLocation", "path/to/HDFS2/dir"+num)
                            .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                            .trigger(Trigger.Continuous(triggerTime))
                            .start();
                }
                else
                {
                    query = result.writeStream()
                            .format("json")
                            .options(options)
                            .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                            .option("checkpointLocation", "path/to/HDFS2/dir"+num)
                            .trigger(Trigger.ProcessingTime(triggerTime))
                            .start();
                }
            }
        }
        else
        {
            System.out.println("触发器未配置，使用默认触发器");
            query = result.writeStream()
                    .format("json")
                    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                    .option("checkpointLocation", "path/to/HDFS2/dir"+num)
                    .options(options)
                    .start();
        }
        return query;
    }

    @Override
    public void checkConfig() {
        if(jsonMap.containsKey("trigger")||jsonMap.containsKey("continuetrigger"))
        {
            isTrigger = true;
            if(jsonMap.containsKey("trigger"))
            {
                String time = jsonMap.get("trigger").toString();
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
                triggerTime = jsonMap.get("continuetrigger").toString();
            }
        }
    }

    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        return null;
    }
}
