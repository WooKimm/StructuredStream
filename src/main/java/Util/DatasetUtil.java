package Util;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import net.sf.json.JSONObject;
import org.apache.spark.unsafe.types.UTF8String;
import parser.CreateTableParser;
import parser.SqlTree;
import scala.Tuple2;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class DatasetUtil {
    //将生成的datastream转化为具有schema的形式
    public static Dataset<Row> getSchemaDataSet(Dataset<Row> dataset, String fieldsString, Boolean isProcess, Map proMap, int id){
        List<ColumnType> columnList = SqlTree.columnLists.get(id);
        List<StructField> fields = new ArrayList<>();
        String[] fieldsArray = fieldsString.split("," + "(?![^()]*+\\))(?![^{}]*+})(?![^\\[\\]]*+\\])(?=(?:[^\"]|\"[^\"]*\")*$)");

        MetadataBuilder b = new MetadataBuilder();
        for(int i = 0; i < fieldsArray.length; i++)
        {
            String fieldRow = fieldsArray[i].trim();
            String[] filedInfoArr = fieldRow.split("\\s+");
            if (filedInfoArr.length < 2) {
                throw new RuntimeException("the legth of " + fieldRow + " is not right");
            }
            String filedName = filedInfoArr[0].toLowerCase();
            String filedType = filedInfoArr[1].toLowerCase();

            columnList.add(new ColumnType(filedName,filedType));
            StructField field = DataTypes.createStructField(filedName, strConverDataType(filedType), true, b.build());
            fields.add(field);
        }

        if (isProcess) {
            StructField field = DataTypes.createStructField("mytimestamp", strConverDataType("timestamp"), true, b.build());
            fields.add(field);
            columnList.add(new ColumnType("mytimestamp", "timestamp"));
        }
        StructType schema = DataTypes.createStructType(fields);

        Dataset<String> schemaDataSet = null;

        if(isProcess)
        {
            schemaDataSet = dataset
                    .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                    .mapPartitions(new MapPartitionsFunction<Tuple2<String, Timestamp>, String>() {
                        @Override
                        public Iterator<String> call(Iterator<Tuple2<String, Timestamp>> input) throws Exception {
                            List<String> recordList = new ArrayList<>();
                            JSONObject jsonObject = new JSONObject();
                            List<ColumnType> columnList = SqlTree.columnLists.get(id);
                            while (input.hasNext()) {
                                Tuple2<String, Timestamp> record = input.next();
                                String[] split = record._1.split(SqlTree.delimiters.get(id));
                                for (int i = 0; i < split.length; i++) {
                                    try {
                                        jsonObject.put(columnList.get(i).getName(), strConverType((split[i]), columnList.get(i).getAttribute()));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        System.out.println("erro line:" + record);
                                    }
                                }
                                //process时，手动加上timestamp，所以这里schema加上timestamp
                                jsonObject.put("mytimestamp", record._2.toString());
                                recordList.add(jsonObject.toString());
                            }
                            return recordList.iterator();
                        }
                    }, Encoders.STRING());
        }
        else
        {
            schemaDataSet = dataset
                    .as(Encoders.STRING())
                    .flatMap(new FlatMapFunction<String, String>() {
                        @Override
                        public Iterator<String> call(String input) throws Exception {
                            List<String> recordList = new ArrayList<>();
                            JSONObject jsonObject = new JSONObject();
                            List<ColumnType> columnList = SqlTree.columnLists.get(id);
                                String record = input;
                                String[] split = record.split(SqlTree.delimiters.get(id));
                                for (int i = 0; i < split.length; i++) {
                                    try {
                                        jsonObject.put(columnList.get(i).getName(), strConverType((split[i]), columnList.get(i).getAttribute()));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        System.out.println("erro line:" + record);
                                    }
                                }
                                recordList.add(jsonObject.toString());
                            return recordList.iterator();
                        }
                    }, Encoders.STRING());
        }

        String[] seExpr = new String[columnList.size()];
        for (int i = 0; i < columnList.size(); i++) {
            seExpr[i] = "v." + columnList.get(i).getName();
        }

        Dataset<Row> transDataSet = schemaDataSet
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema).as("v"))
                .selectExpr(seExpr);

        //window解析相关
        ColumnType windowType = getWindowType(proMap);

        Dataset<Row> result = getDatasetWithWindow(transDataSet, windowType, proMap);

        //return transDataSet;
        return result;
    }


    public static DataType strConverDataType(String filedType) {
        switch (filedType) {
            case "boolean":
                return DataTypes.BooleanType;
            case "int":
                //2019.4.26
            case "video":
                return DataTypes.IntegerType;

            case "bigint":
                return DataTypes.LongType;

            case "tinyint":
            case "byte":
                return DataTypes.ByteType;

            case "short":
            case "smallint":
                return DataTypes.ShortType;

            case "char":
            case "varchar":
            case "string":
                return DataTypes.StringType;

            case "float":
                return DataTypes.FloatType;

            case "double":
                return DataTypes.DoubleType;

            case "date":
                return DataTypes.DateType;

            case "timestamp":
                return DataTypes.TimestampType;

        }

        throw new RuntimeException("不支持 " + filedType + " 类型");
    }

    public static Object strConverType(String str, String filedType) {
        switch (filedType.toLowerCase()) {
            case "boolean":
                return Boolean.valueOf(str);
            case "int":
                return Integer.valueOf(str);

            case "bigint":
                return Long.valueOf(str);

            case "tinyint":
            case "byte":
                return Byte.valueOf(str);

            case "short":
            case "smallint":
                return Short.valueOf(str);

            case "char":
            case "varchar":
            case "string":
                return str;

            case "float":
                return Float.valueOf(str);

            case "double":
                return Double.valueOf(str);

            case "date":
                return Date.valueOf(str);

            case "timestamp":
                return str;
            //这里不转为timestamp的原因是：后面是采用 转json再from_json的方式
            //转的话，后面会解析不出来，倒是没有数据的情况，大坑，已踩
            //return Timestamp.valueOf(str);

        }

        throw new RuntimeException("字符串" + str + "不能转化为" + filedType + " 类型");
    }

    public static Dataset<Row> getDatasetWithWindow(Dataset<Row> transDataSet, ColumnType windowType, Map<String, Object> proMap) {

        Dataset<Row> windowData = null;
        String timeField = "mytimestamp";
        Dataset<Row> waterMarkData = null;
        Boolean isProcess = (Boolean) proMap.get("isProcess");
        if (proMap.containsKey("watermark")) {
            if (!proMap.containsKey("eventfield") && !isProcess) {
                throw new RuntimeException("配置了event的watermark需要配置一个eventfield来和它配合呦");
            }
            if (isProcess) {
                waterMarkData = transDataSet.withWatermark("mytimestamp", proMap.get("watermark").toString());
            } else {
                timeField = proMap.get("eventfield").toString().toLowerCase();
                waterMarkData = transDataSet.withWatermark(timeField, proMap.get("watermark").toString());
            }
        } else {
            waterMarkData = transDataSet;
        }
        //waterMarkData.printSchema();
        if (windowType!=null) {
            String[] splitTime = windowType.getAttribute().split(",");
            String windowDuration = "5 seconds";
            String slideDuration = "5 seconds";
            if (splitTime.length == 1) {
                windowDuration = splitTime[0].trim();
                slideDuration = splitTime[0].trim();
            } else if (splitTime.length == 2) {
                windowDuration = splitTime[0].trim();
                slideDuration = splitTime[1].trim();
            } else {
                throw new RuntimeException("window的配置的长度好像有点问题呦");
            }
            switch (windowType.getName()) {
                case "event":
                    windowData = waterMarkData.withColumn("eventwindow", functions.window(waterMarkData.col(timeField), windowDuration, slideDuration));
                    break;
                case "process":
                    windowData = waterMarkData.withColumn("processwindow", functions.window(waterMarkData.col("mytimestamp"), windowDuration, slideDuration));
                    break;
                default:
                    windowData = waterMarkData;
                    break;
            }
        } else {
            windowData = transDataSet;
        }

        return windowData;
    }

    public static ColumnType getWindowType(Map<String, Object> proMap) {
        String proWindow = "";
        String eventWindow = "";
        try {
            //判断是process类型
            proWindow = proMap.get("processwindow").toString();
            //判断window类型
        } catch (Exception e) {
        }
        try {
            //判断是event类型
            eventWindow = proMap.get("eventwindow").toString();
            //判断window类型
        } catch (Exception e) {
        }

        if (proWindow.length() > 1) {
            return new ColumnType("process",proWindow);
        }
        if (eventWindow.length() > 1) {
            return new ColumnType("event",eventWindow);
        }
        return null;
    }


    public static List<StructField> GetField(CreateTableParser.SqlParserResult config){
        List<StructField> fields = new ArrayList<>();

        //get field and type info
        String fieldsInfoStr = config.getFieldsInfoStr();
        //获取具有schema的dataset
        String[] fieldRows = SplitSql.splitIgnoreQuotaBrackets(fieldsInfoStr, ",");
        MetadataBuilder b = new MetadataBuilder();
        for (String fieldRow : fieldRows) {
            fieldRow = fieldRow.trim();
            String[] filedInfoArr = fieldRow.split("\\s+");
            if (filedInfoArr.length < 2) {
                throw new RuntimeException("the legth of " + fieldRow + " is not right");
            }
            //Compatible situation may arise in space in the fieldName
            String filedName = filedInfoArr[0].toLowerCase();
            String filedType = filedInfoArr[1].toLowerCase();
            StructField field = DataTypes.createStructField(filedName, SplitSql.strConverDataType(filedType), true);
            fields.add(field);
        }
        return fields;
    }
}
