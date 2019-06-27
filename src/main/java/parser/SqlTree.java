package parser;

import net.sf.json.JSONObject;

import java.util.*;

import static Util.SplitSql.newHashSet;

//解析sql后获得的树形结构
public class SqlTree {
    private Set<CreateFuncParser.SqlParserResult> functionList = newHashSet();
    private Map<String, CreateTableParser.SqlParserResult> preDealTableMap = new HashMap<>();
    private Map<String, CreateTableParser.SqlParserResult> preDealSinkMap = new HashMap<>();
    private Map<String, Object> preDealSparkEnvMap = new HashMap<>();
    private Map<String, TableInfo> tableInfoMap = new LinkedHashMap<>();
    private Set<InsertSqlParser.SqlParseResult> execSqlList = newHashSet();
    private InsertSqlParser.SqlParseResult execSql;
    private String appInfo;

    //储存各源表的分隔符和列属性
    public static ArrayList<String> delimiters = new ArrayList<>();
    public static ArrayList<List> columnLists = new ArrayList<>();

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

    public Set<InsertSqlParser.SqlParseResult> getExecSqlList() { return execSqlList; }

    public void setExecSqlList(Set<InsertSqlParser.SqlParseResult> execSqlList) {
        this.execSqlList = execSqlList;
    }

    public InsertSqlParser.SqlParseResult getExecSql() {
        return execSql;
    }

    public void setExecSql(InsertSqlParser.SqlParseResult execSql) {
        this.execSql = execSql;
        this.execSqlList.add(execSql);
    }

    public String getAppInfo() {
        return appInfo;
    }

    public void setAppInfo(String appInfo) {
        this.appInfo = appInfo;
    }


    public void addFunc(CreateFuncParser.SqlParserResult result) {
        functionList.add(result);
    }

    public void addPreDealSparkEnvInfo(Map<String, Object> sparkEnv) {
        sparkEnv.forEach((key,value)->{
            preDealSparkEnvMap.put(key,value);
        });
    }

    public void addPreDealTableInfo(String tableName, CreateTableParser.SqlParserResult table) {
        preDealTableMap.put(tableName,table);
    }

    public void addPreDealSinkInfo(String tableName, CreateTableParser.SqlParserResult table) {
        preDealSinkMap.put(tableName,table);
    }
}

