package Source;

import Util.ColumnType;
import Util.DatasetUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import parser.CreateTableParser;

import java.util.List;
import java.util.Map;

import static Util.DatasetUtil.getWindowType;

public class OrcInput implements BaseInput{
    Map<String, Object> orcMap = null;
    StructType schema = null;
    Dataset<Row> result = null;
    CreateTableParser.SqlParserResult config = null;
    Boolean isProcess = false;
    int id;

    //生成datastream
    @Override
    public Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config,int id) {
        orcMap = config.getPropMap();
        this.id = id;
        this.config = config;
        checkConfig();
        beforeInput();
        List<StructField> fields = DatasetUtil.GetField(config);


        schema = DataTypes.createStructType(fields);

        result = prepare(spark);

        return result;

    }



    //给一个默认的分隔符
    @Override
    public void beforeInput() {
        String delimiter = null;
        try {
            delimiter = orcMap.get("delimiter").toString();
        } catch (Exception e) {
            System.out.println("分隔符未配置，默认为逗号");
        }
        if (delimiter == null) {
            orcMap.put("delimiter", ",");
        }
        if (orcMap.containsKey("processwindow")) {
            isProcess = true;
            orcMap.put("isProcess", true);
        }
        if (orcMap.containsKey("eventwindow")) {
            isProcess = false;
            orcMap.put("isProcess", false);
        }
    }

    @Override
    public void afterInput(){
        final String delimiter = orcMap.get("delimiter").toString();
        if(isProcess){
            try {
//                result.withColumn("timestamp",addCol.call(1));
            } catch (Exception e) {


            }
            return;
        }
        ColumnType windowType = getWindowType(orcMap);
        result = DatasetUtil.getDatasetWithWindow(result, windowType, orcMap);

    }

    //检查config
    @Override
    public void checkConfig() {
        Boolean isValid = orcMap.containsKey("path") &&
                !orcMap.get("path").toString().trim().isEmpty();
        if (!isValid) {
            throw new RuntimeException("path are needed in csvinput input.txt and cant be empty");
            //System.exit(-1);
        }
    }

    //将生成的datastream转化为具有field的形式
    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        Dataset<Row> lines = spark
                .readStream()
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .schema(schema)
                .orc(orcMap.get("path").toString());
        return lines;
    }

    public String getName() {
        return "name";
    }
}

