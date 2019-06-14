package parser;

import java.util.Map;

//解析sql后获得的树形结构
public class SqlTree {
    private Set<CreateFuncParser.SqlParserResult> functionList;
    private Map<String, CreateTableParser.SqlParserResult> preDealTableMap;
    private Map<String, CreateTableParser.SqlParserResult> preDealSinkMap;
    private Map<String, Object> preDealSparkEnvMap;
    private Map<String, TableInfo> tableInfoMap;
    private Set<InsertSqlParser.SqlParseResult> execSqlList;
    private InsertSqlParser.SqlParseResult execSql;
    private String appInfo;

    public Set<CreateFuncParser.SqlParserResult> getFunctionList() {
        return functionList;
    }

    public void setFunctionList(Set<CreateFuncParser.SqlParserResult> functionList) {
        this.functionList = functionList;
    }

    public Map<String, CreateTableParser.SqlParserResult> getPreDealTableMap() {
        return preDealTableMap;
    }

    public void setPreDealTableMap(Map<String, CreateTableParser.SqlParserResult> preDealTableMap) {
        this.preDealTableMap = preDealTableMap;
    }

    public Map<String, CreateTableParser.SqlParserResult> getPreDealSinkMap() {
        return preDealSinkMap;
    }

    public void setPreDealSinkMap(Map<String, CreateTableParser.SqlParserResult> preDealSinkMap) {
        this.preDealSinkMap = preDealSinkMap;
    }

    public Map<String, Object> getPreDealSparkEnvMap() {
        return preDealSparkEnvMap;
    }

    public void setPreDealSparkEnvMap(Map<String, Object> preDealSparkEnvMap) {
        this.preDealSparkEnvMap = preDealSparkEnvMap;
    }

    public Map<String, TableInfo> getTableInfoMap() {
        return tableInfoMap;
    }

    public void setTableInfoMap(Map<String, TableInfo> tableInfoMap) {
        this.tableInfoMap = tableInfoMap;
    }

    public Set<InsertSqlParser.SqlParseResult> getExecSqlList() {
        return execSqlList;
    }

    public void setExecSqlList(Set<InsertSqlParser.SqlParseResult> execSqlList) {
        this.execSqlList = execSqlList;
    }

    public InsertSqlParser.SqlParseResult getExecSql() {
        return execSql;
    }

    public void setExecSql(InsertSqlParser.SqlParseResult execSql) {
        this.execSql = execSql;
    }

    public String getAppInfo() {
        return appInfo;
    }

    public void setAppInfo(String appInfo) {
        this.appInfo = appInfo;
    }
}

