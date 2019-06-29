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
import parser.SqlTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static Util.DatasetUtil.getWindowType;

public class JsonInput implements BaseInput{
    Map<String, Object> jsonMap = null;
    StructType schema = null;
    Dataset<Row> result = null;
    CreateTableParser.SqlParserResult config = null;
    Boolean isProcess = false;
    int id;

    //生成datastream
    @Override
    public Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config, int id) {
        jsonMap = config.getPropMap();
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
            delimiter = jsonMap.get("delimiter").toString();
        } catch (Exception e) {
            System.out.println("分隔符未配置，默认为逗号");
        }
        if (delimiter == null) {
            jsonMap.put("delimiter", ",");
        }
        if (jsonMap.containsKey("processwindow")) {
            isProcess = true;
            jsonMap.put("isProcess", true);
        }
        if (jsonMap.containsKey("eventwindow")) {
            isProcess = false;
            jsonMap.put("isProcess", false);
        }
    }

    @Override
    public void afterInput(){
        String delimiter = jsonMap.get("delimiter").toString();
        SqlTree.delimiters.add(delimiter);
        List<ColumnType> column = new ArrayList<>();
        SqlTree.columnLists.add(column);
        result = DatasetUtil.getSchemaDataSet(result, config.getFieldsInfoStr(), isProcess, config.getPropMap(), id);

    }

    //检查config
    @Override
    public void checkConfig() {
        Boolean isValid = jsonMap.containsKey("path") &&
                !jsonMap.get("path").toString().trim().isEmpty();
        if (!isValid) {
            throw new RuntimeException("path are needed in csvinput input and cant be empty");
            //System.exit(-1);
        }
    }

    //将生成的datastream转化为具有field的形式
    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        Dataset<Row> lines = spark
                .readStream()
//                .option("sep", jsonMap.get("delimiter").toString())
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .schema(schema)
                // Specify schema of the csv files  是个文件目录
                .json(jsonMap.get("path").toString());
        return lines;
    }

    public String getName() {
        return "name";
    }
}
