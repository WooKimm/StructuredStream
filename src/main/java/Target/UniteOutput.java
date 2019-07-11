package Target;

import Util.TestForeachWriter;
import Util.VideoPlayer;
import Util.VideoWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import parser.CreateTableParser;
import parser.InsertSqlParser;

import java.util.HashMap;
import java.util.Map;

public class UniteOutput implements BaseOutput{
    String outputmode = "complete";
    Boolean isTrigger = false;
    Boolean isOnce = false;
    Boolean isContinue = false;
    String triggerTime = "";
    Map<String, Object> Map = null;
    String outputType = "";

    @Override
    public StreamingQuery process(SparkSession spark, Map<String,Dataset<Row>> tablelist, CreateTableParser.SqlParserResult config, InsertSqlParser.SqlParseResult execSql) {
        Map = config.getPropMap();

        checkConfig();

        String querySql = execSql.getQuerySql();

        for (String key: tablelist.keySet()) {
            Dataset<Row> sourceTable = tablelist.get(key);
            sourceTable.createOrReplaceTempView(key);
        }
        Dataset<Row> result = spark.sql(querySql);

        //生成query
        StreamingQuery query = null;
        switch (outputType){
            case "csv":
            case "json":
            case "orc":
            case "parquet":
                query = generateQueryWithFile(outputType,result);
                break;
            case "console":
                query = generateQueryWithConsole(result);
                break;
            case "kafka":
                query = generateQueryWithKafka(result);
                break;
            case "image":
                query = generateQueryWithImage(result);
                break;
        }
        return query;
    }


    @Override
    public void checkConfig() {
        outputType = (String) Map.get("type");
        if(Map.containsKey("outputmode"))
        {
            outputmode = Map.get("outputmode").toString();
        }
        else
        {
            System.out.println("输出模式未配置，默认为完整模式");
        }
        if(Map.containsKey("trigger")||Map.containsKey("continuetrigger"))
        {
            isTrigger = true;
            if(Map.containsKey("trigger"))
            {
                String time = Map.get("trigger").toString();
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
                triggerTime = Map.get("continuetrigger").toString();
            }
        }
    }

    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        return null;
    }


    public StreamingQuery generateQueryWithConsole(Dataset<Row> result){
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

    private StreamingQuery generateQueryWithFile(String outputType, Dataset<Row> result) {
        StreamingQuery query = null;
        String num = String.valueOf((int)(1+Math.random()*(100-1+1)));
        if(isTrigger)
        {
            if(isContinue)
            {
                query = result.writeStream()
                        .format(outputType)
                        .option("path",Map.get("path").toString())
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
                            .format(outputType)
                            .option("path",Map.get("path").toString())
                            .option("checkpointLocation", "path/to/HDFS2/dir"+num)
                            .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                            .trigger(Trigger.Once())
                            .start();
                }
                else
                {
                    query = result.writeStream()
                            .format(outputType)
                            .option("path",Map.get("path").toString())
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
                    .format(outputType)
                    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                    .option("checkpointLocation", "path/to/HDFS2/dir"+num)
                    .option("path",Map.get("path").toString())
                    .start();
        }
        return query;
    }

    private StreamingQuery generateQueryWithKafka(Dataset<Row> result) {
        Map<String, String> options = new HashMap<>();
        options.put("kafka.bootstrap.servers", Map.get("kafka.bootstrap.servers").toString());
        options.put("topic", Map.get("topic").toString());
        StreamingQuery query = null;
        TestForeachWriter writer = new TestForeachWriter(Map.get("kafka.bootstrap.servers").toString(),Map.get("topic").toString());
        if(isTrigger)
        {
            if(isContinue)
            {
//                query = result
//                        .selectExpr("CAST(word AS STRING)")
//                        .writeStream()
//                        .outputMode(outputmode)
//                        .format("kafka")
//                        .options(options)
//                        .option("checkpointLocation", "path/to/HDFS/dir")
//                        .trigger(Trigger.Continuous(triggerTime))
//                        .start();
                query = result
                        .writeStream()
                        .foreach(writer)
                        .outputMode(outputmode)
                        .trigger(Trigger.Continuous(triggerTime))
                        .start();
            }
            else
            {
                if(isOnce)
                {
                    query = result
                            .writeStream()
                            .foreach(writer)
                            .outputMode(outputmode)
                            .trigger(Trigger.Once())
                            .start();
                }
                else
                {
                    query = result
                            .writeStream()
                            .foreach(writer)
                            .outputMode(outputmode)
                            .trigger(Trigger.ProcessingTime(triggerTime))
                            .start();
                }
            }
        }
        else
        {
            System.out.println("触发器未配置，使用默认触发器");
            query = result
                    .writeStream()
                    .foreach(writer)
                    .outputMode(outputmode)
                    .start();
        }
        return query;
    }

    private StreamingQuery generateQueryWithImage(Dataset<Row> result) {
        StreamingQuery query = null;

//        VideoPlayer videoPlayer = new VideoPlayer("videoplayer");
//        videoPlayer.start();

        if(isTrigger)
        {
            if(isContinue)
            {
                query = result.writeStream()
                        .outputMode(outputmode)
                        .foreach(new VideoWriter())
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
                            .foreach(new VideoWriter())
                            .trigger(Trigger.Once())
                            .option("truncate", false)
                            .start();
                }
                else
                {
                    query = result.writeStream()
                            .outputMode(outputmode)
                            .foreach(new VideoWriter())
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
                    .foreach(new VideoWriter())
                    .option("truncate", false)
                    .start();
        }
        return query;
    }


}
