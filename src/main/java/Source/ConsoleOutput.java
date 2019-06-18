package Source;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import parser.CreateTableParser;
import parser.InsertSqlParser;
import parser.SqlTree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ConsoleOutput implements BaseOutput{
    String outputmode = "complete";
    Map<String, Object> consoleMap = null;
    @Override
    public StreamingQuery process(SparkSession spark, Dataset<Row> dataset, CreateTableParser.SqlParserResult config, SqlTree sqlTree) {
        InsertSqlParser.SqlParseResult execSql = sqlTree.getExecSql();
        consoleMap = config.getPropMap();
        checkConfig();

        StreamingQuery streamingQuery = null;

        String querySql = execSql.getQuerySql();
        String[] operators = querySql.split(" ");
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
        for(String columnName : groupers)
        {
            if(columnName.equals("processwindow")||columnName.equals("eventwindow"))
            {
                groupColumns.add(functions.window(dataset.col("timestamp"), windowDuration, slideDuration));
            }
            else
            {
                groupColumns.add(new Column(columnName));
            }
        }
        Dataset<Row> befSelectResult = dataset.groupBy((Column[])groupColumns.toArray()).count();

        //然后写select
        ArrayList<Column> selectColumns = new ArrayList<>();
        for(String columnName : selectors)
        {
            if(columnName.equals("count(*)"))
            {
                groupColumns.add(new Column("count"));
            }
            else
            {
                groupColumns.add(new Column(columnName));
            }
        }
        Dataset<Row> aftSelectResult = befSelectResult.select((Column[])selectColumns.toArray());

        //生成query
        StreamingQuery query = aftSelectResult.writeStream()
                .outputMode(outputmode)
                .format("console")
                .trigger(Trigger.ProcessingTime(windowDuration))
                .start();

        return streamingQuery;
    }

    @Override
    public void checkConfig() {
        if(consoleMap.containsKey("outputmode"))
        {
            outputmode = consoleMap.get("outputmode").toString();
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
