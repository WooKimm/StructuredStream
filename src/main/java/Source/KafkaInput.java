package Source;

import Util.ColumnType;
import Util.DatasetUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import parser.CreateTableParser;
import parser.SqlTree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static Util.SchemaDataset.GetSchemaDataSet;


public class KafkaInput implements BaseInput{

    Map<String, Object> kafkaMap = null;
    String windowType = "";
    CreateTableParser.SqlParserResult config = null;
    Boolean isProcess = false;
    Dataset<Row> result = null;
    int id;

    @Override
    public Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config, int id) {
        kafkaMap = config.getPropMap();
        this.id = id;
        this.config = config;
        checkConfig();
        beforeInput();
        //获取prepare后具有field的dataset
        result = prepare(spark);
        //获取具有schema和window的dataset
        afterInput();
        return result;
    }

    @Override
    public void beforeInput() {
        String delimiter = null;
        try {
            delimiter = kafkaMap.get("delimiter").toString();
        } catch (Exception e) {
            System.out.println("分隔符未配置，默认为逗号");
        }
        if (delimiter == null) {
            kafkaMap.put("delimiter", ",");
        }
        if (kafkaMap.containsKey("processwindow")) {
            isProcess = true;
            kafkaMap.put("isProcess", true);
        }
        if (kafkaMap.containsKey("eventwindow")) {
            isProcess = false;
            kafkaMap.put("isProcess", false);
        }
    }

    @Override
    public void afterInput() {

        String delimiter = kafkaMap.get("delimiter").toString();
        SqlTree.delimiters.add(delimiter);
        List<ColumnType> column = new ArrayList<>();
        SqlTree.columnLists.add(column);

        result = DatasetUtil.getSchemaDataSet(result, config.getFieldsInfoStr(), isProcess, config.getPropMap(), id);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void checkConfig() {
        Boolean isValid = kafkaMap.containsKey("subscribe") &&
                !kafkaMap.get("subscribe").toString().trim().isEmpty() &&
                kafkaMap.containsKey("kafka.bootstrap.servers") &&
                !kafkaMap.get("kafka.bootstrap.servers").toString().trim().isEmpty() &&
                kafkaMap.containsKey("group") &&
                !kafkaMap.get("group").toString().trim().isEmpty();
        if (!isValid) {
            throw new RuntimeException("topics and kafka.bootstrap.servers and group.id are needed in kafka input and cant be empty");
            //System.exit(-1);
        }
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
