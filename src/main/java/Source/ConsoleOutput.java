package Source;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import parser.CreateTableParser;
import parser.InsertSqlParser;
import parser.SqlTree;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class ConsoleOutput implements BaseOutput{
    String outputmode = "complete";
    Map<String, Object> consoleMap = null;
    Map<String, Object> propMap = null;
    @Override
    public StreamingQuery process(SparkSession spark, Dataset<Row> dataset, CreateTableParser.SqlParserResult config, SqlTree sqlTree) {
        InsertSqlParser.SqlParseResult execSql = sqlTree.getExecSql();
        consoleMap = config.getPropMap();
        checkConfig();

        StreamingQuery streamingQuery = null;

        String querySql = execSql.getQuerySql();
        String[] operators = querySql.split(" |\r\n");
        Map<String,String[]> operations = getOperation(operators);

        String[] selectors = null;
        String[] groupers = null;
        String[] sourceTables = null;
        Boolean isCount = false;
        String windowDuration = "";
        String slideDuration = "";
        //先count和group by
        if(operations.containsKey("select"))
        {
            selectors = operations.get("select");
            for (String selector : selectors) {
                if(selector.equals("count(*)"))
                {
                    isCount = true;
                }
            }
        }
        if(operations.containsKey("group"))
        {
            groupers = operations.get("group");
        }
        if(operations.containsKey("from"))
        {
            sourceTables = operations.get("from");
            for (String table: sourceTables) {
                Map createTableMap = sqlTree.getPreDealTableMap().get(table).getPropMap();
                String durations;
                if(createTableMap.containsKey("processwindow"))
                {
                    durations = createTableMap.get("processwindow").toString();
                }
                else//eventwindow
                {
                    durations = createTableMap.get("eventwindow").toString();
                }
                windowDuration = durations.substring(0,durations.indexOf(","));
                slideDuration = durations.substring(durations.indexOf(",")+1);
            }

        }
        ArrayList<Column> groupColumns = new ArrayList<>();
//        Column[] groupColumns = new Column[10];
        for(String columnName : groupers)
        {
            if(columnName.equals("processwindow")||columnName.equals("eventwindow"))
            {
                groupColumns.add(functions.window(dataset.col("timestamp"), windowDuration, slideDuration));

            }
            else
            {
                groupColumns.add(dataset.col(columnName));
            }
        }
        Column[] groupColumnArray = new Column[groupColumns.size()];

        Dataset<Row> befSelectResult = dataset.groupBy(groupColumns.toArray(groupColumnArray)).count();

        //然后写select
        ArrayList<Column> selectColumns = new ArrayList<>();
        for(String columnName : selectors)
        {
            if(columnName.equals("count(*)"))
            {
                selectColumns.add(befSelectResult.col("count"));
            }
            else if(columnName.equals("processwindow")||columnName.equals("eventwindow")){
                selectColumns.add(befSelectResult.col("window"));
            }
            else
            {
                selectColumns.add(befSelectResult.col(columnName));
            }
        }
        Column[] selectColumnArray = new Column[selectColumns.size()];
        Dataset<Row> aftSelectResult = befSelectResult.select(selectColumns.toArray(selectColumnArray));

        //生成query
        StreamingQuery query = aftSelectResult.writeStream()
                .outputMode(outputmode)
                .format("console")
                .trigger(ProcessingTime(windowDuration))
                .start();

        return query;
    }

    @Override
    public StreamingQuery process2(SparkSession spark, Dataset<Row> dataset, CreateTableParser.SqlParserResult config) {
        StreamingQuery query = null;

        propMap = config.getPropMap();
        checkConfig();
        //TODO 后面改进 情况可能更多trigger(Trigger.Continuous("1 second")) trigger(Trigger.Once())
        if (propMap.containsKey("process")) {
            String process = getProcessTime(propMap.get("process").toString());
            query = dataset.writeStream()
                    .outputMode(propMap.get("outputmode").toString())
                    .format("console")
                    .option("truncate", "false")
                    .trigger(ProcessingTime(process))
                    .start();
        } else {
            query = dataset.writeStream()
                    .outputMode(propMap.get("outputmode").toString())
                    .option("truncate", "false")
                    .format("console")
                    .start();
        }

        return query;
    }
    public static String getProcessTime(String proTimeStr) {
        //String processTime = "2 seconds";
        String number;
        String time;
        number = proTimeStr.replaceAll("[^(0-9)]", "");
        time = proTimeStr.replaceAll("[^(A-Za-z)]", "");
        switch (time) {
            case "s":
            case "S":
                return number + " seconds";
        }
        throw new RuntimeException("process的时间是非法的");
    }



    @Override
    public void checkConfig() {
        if(propMap.containsKey("outputmode"))
        {
            outputmode = propMap.get("outputmode").toString();
        }
    }

    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        return null;
    }

    private Map<String, String[]> getOperation(String[] operators)
    {
        Map<String, String[]> operations = new HashMap<>();
        for (int i = 0; i < operators.length; i++)
        {
            String operator = operators[i];
            String[] values;
            switch (operator)
            {
                case "select":
                    values = operators[++i].split(",");
                    operations.put(operator,values);
                    break;
                case "from":
                    values = operators[++i].split(",");
                    operations.put(operator,values);
                    break;
                case "group":
                    i++;
                    values = operators[++i].split(",");
                    operations.put(operator,values);
                    break;
                default:
                    break;
            }
        }
        return operations;
    }
}
