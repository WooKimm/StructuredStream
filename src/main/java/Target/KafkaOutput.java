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

public class KafkaOutput implements BaseOutput{
    String outputmode = "complete";
    Boolean isTrigger = false;
    Boolean isOnce = false;
    Boolean isContinue = false;
    String triggerTime = "";
    Map<String, Object> kafkaMap = null;

    @Override
    public StreamingQuery process(SparkSession spark, Map<String,Dataset<Row>> tablelist, CreateTableParser.SqlParserResult config, InsertSqlParser.SqlParseResult execSql) {
        kafkaMap = config.getPropMap();
        checkConfig();

        String querySql = execSql.getQuerySql();

        for (String key: tablelist.keySet()) {
            Dataset<Row> sourceTable = tablelist.get(key);
            sourceTable.createOrReplaceTempView(key);
        }
        Dataset<Row> result = spark.sql(querySql);

        Map<String, String> options = new HashMap<>();
        options.put("kafka.bootstrap.servers", kafkaMap.get("kafka.bootstrap.servers").toString());
        options.put("topic", kafkaMap.get("topic").toString());

        //生成query
        StreamingQuery query = null;
        if(isTrigger)
        {
            if(isContinue)
            {
                query = result.writeStream()
                        .outputMode(outputmode)
                        .format("kafka")
                        .options(options)
                        .option("checkpointLocation", "path/to/HDFS/dir")
                        .trigger(Trigger.Continuous(triggerTime))
                        .start();
            }
            else
            {
                if(isOnce)
                {
                    query = result.writeStream()
                            .outputMode(outputmode)
                            .format("kafka")
                            .options(options)
                            .option("checkpointLocation", "path/to/HDFS/dir")
                            .trigger(Trigger.Continuous(triggerTime))
                            .start();
                }
                else
                {
                    query = result.writeStream()
                            .outputMode(outputmode)
                            .format("kafka")
                            .options(options)
                            .option("checkpointLocation", "path/to/HDFS/dir")
                            .trigger(Trigger.ProcessingTime(triggerTime))
                            .start();
                }
            }
        }
        else
        {
            System.out.println("触发器未配置，使用默认触发器");
            query = result.writeStream()
                    .outputMode(outputmode)
                    .format("kafka")
                    .option("checkpointLocation", "path/to/HDFS/dir")
                    .options(options)
                    .start();
        }
        return query;
    }

    @Override
    public void checkConfig() {
        if(kafkaMap.containsKey("outputmode"))
        {
            outputmode = kafkaMap.get("outputmode").toString();
        }
        else
        {
            System.out.println("输出模式未配置，默认为完整模式");
        }
        if(kafkaMap.containsKey("trigger")||kafkaMap.containsKey("continuetrigger"))
        {
            isTrigger = true;
            if(kafkaMap.containsKey("trigger"))
            {
                String time = kafkaMap.get("trigger").toString();
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
                triggerTime = kafkaMap.get("continuetrigger").toString();
            }
        }
    }

    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        return null;
    }
}
