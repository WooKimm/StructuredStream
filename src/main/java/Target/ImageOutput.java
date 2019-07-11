package Target;

import Util.VideoPlayer;
import Util.VideoWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import parser.CreateTableParser;
import parser.InsertSqlParser;

import java.util.Map;

import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class ImageOutput implements BaseOutput {
    String outputmode = "complete";
    Boolean isTrigger = false;
    Boolean isOnce = false;
    Boolean isContinue = false;
    String triggerTime = "";
    Map<String, Object> imageMap = null;

    @Override
    public StreamingQuery process(SparkSession spark, Map<String,Dataset<Row>> tablelist, CreateTableParser.SqlParserResult config, InsertSqlParser.SqlParseResult execSql) {
        imageMap = config.getPropMap();
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

    @Override
    public void checkConfig() {
        if(imageMap.containsKey("outputmode"))
        {
            outputmode = imageMap.get("outputmode").toString();
        }
        else
        {
            System.out.println("输出模式未配置，默认为完整模式");
        }
        if(imageMap.containsKey("trigger")||imageMap.containsKey("continuetrigger"))
        {
            isTrigger = true;
            if(imageMap.containsKey("trigger"))
            {
                String time = imageMap.get("trigger").toString();
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
                triggerTime = imageMap.get("continuetrigger").toString();
            }
        }
    }

    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        return null;
    }
}
