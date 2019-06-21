package Source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import parser.CreateTableParser;

import java.util.HashMap;
import java.util.Map;

import static Util.SchemaDataset.GetSchemaDataSet;


public class KafkaInput implements BaseInput{

    Map<String, Object> kafkaMap = null;
    String windowType = "";
    Boolean isProcess = true;

    @Override
    public Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config) {
        kafkaMap = config.getPropMap();
        checkConfig();
        beforeInput();
        //获取prepare后具有field的dataset
        Dataset<Row> lineRow = prepare(spark);
        //获取具有schema的dataset
        lineRow = GetSchemaDataSet(lineRow,config);
        //获取window类型并处理后的dataset
//        Dataset<Row> lineRowWithWindow = GetWindowDataset(lineRow,socketMap);
        return lineRow;
    }

    @Override
    public void beforeInput() {

    }

    @Override
    public void afterInput() {

    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void checkConfig() {

    }

    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        Map<String, String> options = new HashMap<>();
        options.put("kafka.bootstrap.servers", kafkaMap.get("kafka.bootstrap.servers").toString());
        options.put("subscribe", kafkaMap.get("subscribe").toString());
        options.put("group.id", kafkaMap.get("group").toString());

        Dataset<Row> rowDataset = null;

        if (isProcess) {
            Dataset<Row> lines = spark
                    .readStream()
                    .format("kafka")
                    .options(options)
                    .option("includeTimestamp", true)
                    .load();
            rowDataset = lines.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)");
        } else {
            Dataset<Row> lines = spark
                    .readStream()
                    .format("kafka")
                    .options(options)
                    .load();
            rowDataset = lines.selectExpr("CAST(value AS STRING)");
        }

        return rowDataset;
    }
}
