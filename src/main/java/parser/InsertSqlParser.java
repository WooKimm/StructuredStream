package parser;

import org.apache.commons.lang3.StringUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import static org.apache.calcite.sql.SqlKind.IDENTIFIER;
import java.util.Set;

import static Util.SplitSql.newHashSet;

//解析以"insert"开头的sql语句，使用Apache Calcite工具
public class InsertSqlParser implements IParser {

    static String querySql = "";

    public boolean verify(String sql) {
        return StringUtils.isNotBlank(sql) && sql.trim().toLowerCase().startsWith("insert");
    }

    public static InsertSqlParser newInstance() {
        InsertSqlParser parser = new InsertSqlParser();
        return parser;
    }

    public void parseSql(String sql, SqlTree sqlTree) {
        SqlParser sqlParser = SqlParser.create(sql);
        SqlNode sqlNode = null;
        try {
            sqlNode = sqlParser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException("", e);
        }

        SqlParseResult sqlParseResult = new SqlParseResult();
        parseNode(sqlNode, sqlParseResult);

        sqlParseResult.setExecSql(sqlNode.toString().toLowerCase().replace("`","").replace(", ",","));
        querySql = querySql.toLowerCase().replace("`","").replace(", ",",");
        sqlParseResult.setQuerySql(querySql);
        //sqlTree.addExecSql(sqlParseResult);
        sqlTree.setExecSql(sqlParseResult);
    }

    private static void parseNode(SqlNode sqlNode, SqlParseResult sqlParseResult) {
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case INSERT:
                SqlNode sqlTarget = ((SqlInsert) sqlNode).getTargetTable();
                SqlNode sqlSource = ((SqlInsert) sqlNode).getSource();
                querySql = sqlSource.toString();
                sqlParseResult.setTargetTable(sqlTarget.toString());
                parseNode(sqlSource, sqlParseResult);
                break;
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect) sqlNode).getFrom();
                if (sqlFrom.getKind() == IDENTIFIER) {
                    sqlParseResult.addSourceTable(sqlFrom.toString());
                } else {
                    parseNode(sqlFrom, sqlParseResult);
                }
                break;
            case JOIN:
                SqlNode leftNode = ((SqlJoin) sqlNode).getLeft();
                SqlNode rightNode = ((SqlJoin) sqlNode).getRight();

                if (leftNode.getKind() == IDENTIFIER) {
                    sqlParseResult.addSourceTable(leftNode.toString());
                } else {
                    parseNode(leftNode, sqlParseResult);
                }

                if (rightNode.getKind() == IDENTIFIER) {
                    sqlParseResult.addSourceTable(rightNode.toString());
                } else {
                    parseNode(rightNode, sqlParseResult);
                }
                break;
            case AS:
                //不解析column,所以 as 相关的都是表
                SqlNode identifierNode = ((SqlBasicCall) sqlNode).getOperands()[0];
                if (identifierNode.getKind() != IDENTIFIER) {
                    parseNode(identifierNode, sqlParseResult);
                } else {
                    sqlParseResult.addSourceTable(identifierNode.toString());
                }
                break;
            default:
                //do nothing
                break;
        }
    }

    public static class SqlParseResult {

        private Set<String> sourceTableList = newHashSet();

        private Set<String> targetTableList = newHashSet();//输出池应该只支持一个

        private String targetTable;

        private String execSql;

        private String querySql;

        public void addSourceTable(String sourceTable) { sourceTableList.add(sourceTable); }

        public void addTargetTable(String targetTable) { targetTableList.add(targetTable); }

        public Set<String> getSourceTableList() {
            return sourceTableList;
        }

        public Set<String> getTargetTableList() {
            return targetTableList;
        }

        public String getTargetTable() {
            return targetTable;
        }

        public void setTargetTable(String targetTable) {
            this.targetTable = targetTable;
        }

        public String getExecSql() {
            return execSql;
        }

        public void setExecSql(String execSql) {
            this.execSql = execSql;
        }

        public String getQuerySql() {
            return querySql;
        }

        public void setQuerySql(String querySql) {
            this.querySql = querySql;
        }
    }
}

