package Util;

import Source.BaseInput;
import Target.BaseOutput;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import parser.CreateTableParser;

import parser.InsertSqlParser;
import parser.SqlParser;
import parser.SqlTree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SparkUtil {
    static String sourceBasePackage = "Source.";
    static String targetBasePackage = "Target.";
    public static StreamingQuery streamingQuery = null;
    static SparkSession spark = null;


    public static Map<String,Dataset<Row>> createDataFrame(SparkSession spark, SqlTree sqlTree) throws Exception{
        if(sqlTree==null){
            //解析zk传过来的node，即sql语句
            BaseZookeeper zookeeper = new BaseZookeeper();
            zookeeper.connectZookeeper("127.0.0.1:2181");
            SqlParser.parseSql(zookeeper.getData("/sqlTest"));
            sqlTree = SqlParser.sqlTree;
        }
        Map<String, Dataset<Row>> tableList = SparkUtil.getTableList(spark, sqlTree);

        return tableList;

    }
    //添加window函数
    private static Map<String,Dataset<Row>> getTableList(SparkSession spark, SqlTree sqlTree) throws Exception{
        Map<String, Dataset<Row>> rowTableList = new HashMap<>();
        if(sqlTree==null){
            BaseZookeeper zookeeper = new BaseZookeeper();
            zookeeper.connectZookeeper("127.0.0.1:2181");
            SqlParser.parseSql(zookeeper.getData("/sqlTest"));
            sqlTree = SqlParser.sqlTree;
        }
        Map<String, CreateTableParser.SqlParserResult> preDealTableMap = sqlTree.getPreDealTableMap();
        for (String key : preDealTableMap.keySet()) {
            //key是每个table的名字
            String type = (String) sqlTree.getPreDealTableMap().get(key).getPropMap().get("type");
            String upperType = SplitSql.upperCaseFirstChar(type) + "Input";
            //创建BaseInput接口的实现类的对象sourceByClass
            BaseInput sourceByClass = SparkUtil.getSourceByClass(upperType);
            //使用对象的getDataSetStream转换成Dataset
            Dataset<Row> dataset = sourceByClass.getDataSetStream(spark, preDealTableMap.get(key));
            rowTableList.put(key, dataset);
        }
        return rowTableList;
    }
    //通过多态实现不同输入源类型创建不同实现类的对象
    public static BaseInput getSourceByClass(String className) {
        BaseInput inputBase = null;
        try {
            inputBase = Class.forName(sourceBasePackage + className).asSubclass(BaseInput.class).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return inputBase;
    }

    public static StreamingQuery createStreamingQuery(SparkSession spark, SqlTree sqlTree, Map<String,Dataset<Row>> tablelist) throws StreamingQueryException {

        Map<String, CreateTableParser.SqlParserResult> preDealSinkMap = sqlTree.getPreDealSinkMap();

        Set<InsertSqlParser.SqlParseResult> execSqlList = sqlTree.getExecSqlList();
        ArrayList<String> execSqls = new ArrayList<>();
        for(InsertSqlParser.SqlParseResult sqlParseResult : execSqlList)
        {
            execSqls.add(sqlParseResult.getTargetTable());
        }

        StreamingQuery streamingQuery = null;
        for (String key : execSqls) {
            String type = (String) sqlTree.getPreDealSinkMap().get(key).getPropMap().get("type");
            String upperType = SplitSql.upperCaseFirstChar(type) + "Output";
            BaseOutput sinkByClass = SparkUtil.getSinkByClass(upperType);
            streamingQuery = sinkByClass.process(spark,tablelist,preDealSinkMap.get(key), sqlTree);
        }
        return streamingQuery;
    }


    public static BaseOutput getSinkByClass(String className)
    {
        BaseOutput outputBase = null;
        try {
            outputBase = Class.forName(targetBasePackage + className).asSubclass(BaseOutput.class).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return outputBase;
    }

    public static String getSqlFromSource()
    {
        File file = new File("src/main/resources/testSQLFile");
        BufferedReader reader = null;
        String sql = "";
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                tempString = tempString + "\n";
                sql += tempString;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return sql;
    }
}
