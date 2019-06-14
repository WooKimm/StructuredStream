package parser;

import Util.SplitSql;

import java.util.List;

public class SqlParser {
//    protected static List<IParser> sqlParserList = newArrayList(CreateFuncParser.newInstance(),
//            CreateTableParser.newInstance(), InsertSqlParser.newInstance(), CreateSinkParser.newInstance(),CreateEnvParser.newInstance());
    public static void parseSql(String sql){
        sql = sql.replaceAll("--.*", "")
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replace("\t", " ").trim();

        //将整个sql文件按照';'分隔，分成几个sql
        List<String> sqlArr = SplitSql.splitWithSemicolon(sql, ';');
    }
}
