package Source;

import Util.ColumnType;
import Util.DatasetUtil;
import Util.SplitSql;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import parser.CreateTableParser;

import java.util.List;
import java.util.Map;

import static Util.DatasetUtil.getWindowType;



public class CsvInput implements BaseInput {

    Map<String, Object> csvMap = null;
    StructType schema = null;
    Dataset<Row> result = null;
    CreateTableParser.SqlParserResult config = null;
    Boolean isProcess = false;
    int id;

    //生成datastream
    @Override
    public Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config, int id) {
        csvMap = config.getPropMap();
        this.id = id;
        this.config = config;
        checkConfig();
        beforeInput();
        List<StructField> fields = DatasetUtil.GetField(config);

        //schema加上timestamp
        if (isProcess) {
            StructField field = DataTypes.createStructField("mytimestamp", SplitSql.strConverDataType("timestamp"), true);
            fields.add(field);
        }
        DataType stringType = DataTypes.StringType;

        schema = DataTypes.createStructType(fields);

        //获取prepare后具有field的dataset
        result = prepare(spark);
        //获取具有schema和window的dataset
        afterInput();
        return result;

    }

    //给一个默认的分隔符
    @Override
    public void beforeInput() {
        String delimiter = null;
        try {
            delimiter = csvMap.get("delimiter").toString();
        } catch (Exception e) {
            System.out.println("分隔符未配置，默认为逗号");
        }
        if (delimiter == null) {
            csvMap.put("delimiter", ",");
        }
        if (csvMap.containsKey("processwindow")) {
            isProcess = true;
            csvMap.put("isProcess", true);
        }
        if (csvMap.containsKey("eventwindow")) {
            isProcess = false;
            csvMap.put("isProcess", false);
        }
    }

    @Override
    public void afterInput(){

        final String delimiter = csvMap.get("delimiter").toString();
        if(isProcess){
            try {

                result = result.withColumn("mytimestamp",org.apache.spark.sql.functions.current_timestamp());

            } catch (Exception e) {
            }
        }
        ColumnType windowType = getWindowType(csvMap);
        result = DatasetUtil.getDatasetWithWindow(result, windowType, csvMap);


    }

    //检查config
    @Override
    public void checkConfig() {
        Boolean isValid = csvMap.containsKey("path") &&
                !csvMap.get("path").toString().trim().isEmpty();
        if (!isValid) {
            throw new RuntimeException("path are needed in csvinput input and cant be empty");
            //System.exit(-1);
        }
        if (csvMap.containsKey("processwindow")) {
            isProcess = true;
            csvMap.put("isProcess", true);
        }
        if (csvMap.containsKey("eventwindow")) {
            isProcess = false;
            csvMap.put("isProcess", false);
        }
    }

    //将生成的datastream转化为具有field的形式
    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        Dataset<Row> lines = spark
                .readStream()
                .option("sep", csvMap.get("delimiter").toString())
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .schema(schema)
                // Specify schema of the csv files  是个文件目录
                .csv(csvMap.get("path").toString());
        return lines;
    }

    public String getName() {
        return "name";
    }



}
