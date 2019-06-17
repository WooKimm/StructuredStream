package Util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import parser.CreateTableParser;
import parser.SqlParser;
import parser.SqlTree;

import java.util.HashMap;
import java.util.Map;

public class SparkUtil{

    public static BaseInput getSourceByClass(Object type)
    {
        String stype = type.toString();
        switch (stype)
        {
            case "socket":
                return new SocketInput();
            default:
                return null;
        }
    }

    public static BaseOutput getSinkByClass()
    {
        return null;
    }
}
