package parser;

public interface IParser {
    /**
     * 是否满足该解析类型
     */
    boolean verify(String sql);
    /***
     * 解析sql
     */
    void parseSql(String sql, SqlTree sqlTree);

}
