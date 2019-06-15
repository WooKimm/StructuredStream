package parser;

import Util.SplitSql;

import java.util.List;

import static Util.SplitSql.newArrayList;
import static org.spark_project.guava.base.Strings.isNullOrEmpty;

public class SqlParser {
    protected static List<IParser> sqlParserList = newArrayList(CreateFuncParser.newInstance(),
            CreateTableParser.newInstance(), InsertSqlParser.newInstance(), CreateSinkParser.newInstance(),CreateEnvParser.newInstance());
    public static SqlTree sqlTree;

    public static void parseSql(String sql){
        sql = sql.replaceAll("--.*", "")
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replace("\t", " ").trim();

        //将整个sql文件按照';'分隔，分成几个sql
        List<String> sqlArr = SplitSql.splitWithDel(sql, ';');
        SqlTree sqlTree = new SqlTree();

        for (String childSql : sqlArr) {
            if (SplitSql.isNullOrEmpty(childSql)) {
                continue;
            }
            boolean result = false;
            //sqlParserList含有三种解析类型，CreateFuncParser——CreateTableParser——InsertSqlParser
            //为每一个sql查找合适的解析类型

            for (IParser sqlParser : sqlParserList) {
                if (!sqlParser.verify(childSql)) {
                    continue;
                }
                sqlParser.parseSql(childSql, sqlTree);
                result = true;
            }

            if (!result) {
                throw new RuntimeException(String.format("%s:Syntax does not support,the format of SQL like insert into tb1 select * from tb2.", childSql));
            }
        }
        //解析exec-sql
        if (sqlTree.getExecSqlList().size() == 0 && sqlTree.getExecSql() == null) {
            throw new RuntimeException("sql no executable statement");
        }
        SqlParser.sqlTree = sqlTree;
    }
}
