package Source;

import Util.ColumnType;
import Util.DatasetUtil;
import Util.SplitSql;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.jackson.map.Serializers;
import parser.CreateTableParser;

import java.util.List;
import java.util.Map;

import static Util.DatasetUtil.getWindowType;

public class ParquetInput implements BaseInput {
    Map<String, Object> parquetMap = null;
    StructType schema = null;
    Dataset<Row> result = null;
    CreateTableParser.SqlParserResult config = null;
    Boolean isProcess = false;
    int id;

    //生成datastream
    @Override
    public Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config, int id) {
        parquetMap = config.getPropMap();
        this.id = id;
        this.config = config;
        checkConfig();
        beforeInput();
        List<StructField> fields = DatasetUtil.GetField(config);

        if (isProcess) {
            StructField field = DataTypes.createStructField("mytimestamp", SplitSql.strConverDataType("timestamp"), true);
            fields.add(field);
        }
        schema = DataTypes.createStructType(fields);

        result = prepare(spark);
        afterInput();
        return result;

    }



    //给一个默认的分隔符
    @Override
    public void beforeInput() {
        String delimiter = null;
        try {
            delimiter = parquetMap.get("delimiter").toString();
        } catch (Exception e) {
            System.out.println("分隔符未配置，默认为逗号");
        }
        if (delimiter == null) {
            parquetMap.put("delimiter", ",");
        }
        if (parquetMap.containsKey("processwindow")) {
            isProcess = true;
            parquetMap.put("isProcess", true);
        }
        if (parquetMap.containsKey("eventwindow")) {
            isProcess = false;
            parquetMap.put("isProcess", false);
        }
    }

    @Override
    public void afterInput(){
        final String delimiter = parquetMap.get("delimiter").toString();
        if(isProcess){
            try {

                result = result.withColumn("mytimestamp",org.apache.spark.sql.functions.current_timestamp());

            } catch (Exception e) {
            }
        }
        ColumnType windowType = getWindowType(parquetMap);
        result = DatasetUtil.getDatasetWithWindow(result, windowType, parquetMap);

    }

    //检查config
    @Override
    public void checkConfig() {
        Boolean isValid = parquetMap.containsKey("path") &&
                !parquetMap.get("path").toString().trim().isEmpty();
        if (!isValid) {
            throw new RuntimeException("path are needed in csvinput input.txt and cant be empty");
            //System.exit(-1);
        }
        if (parquetMap.containsKey("processwindow")) {
            isProcess = true;
            parquetMap.put("isProcess", true);
        }
        if (parquetMap.containsKey("eventwindow")) {
            isProcess = false;
            parquetMap.put("isProcess", false);
        }
    }

    //将生成的datastream转化为具有field的形式
    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        Dataset<Row> lines = spark
                .readStream()
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .option("mergeSchema", true)
                .schema(schema)
                .parquet(parquetMap.get("path").toString());
        return lines;
    }

    public String getName() {
        return "name";
    }
}
