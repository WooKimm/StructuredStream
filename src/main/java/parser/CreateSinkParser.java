package parser;

import Util.SplitSql;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//解析创建Sink语句，这里的属性与table相同，不再创建新类
public class CreateSinkParser implements IParser {

    private static final String PATTERN_STR = "(?i)create\\s+sink\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    public static CreateSinkParser newInstance() {
        return new CreateSinkParser();
    }

    public boolean verify(String sql) {
        return PATTERN.matcher(sql).find();
    }

    public void parseSql(String sql, SqlTree sqlTree) {
        Matcher matcher = PATTERN.matcher(sql);
        if (matcher.find()) {
            String tableName = matcher.group(1).toUpperCase();
            String fieldsInfoStr = matcher.group(2);
            String propsStr = matcher.group(3);
            Map<String, Object> props = SplitSql.parseProp(propsStr);

            CreateTableParser.SqlParserResult result = new CreateTableParser.SqlParserResult();
            result.setTableName(tableName);
            result.setFieldsInfoStr(fieldsInfoStr);
            result.setPropMap(props);

            sqlTree.addPreDealSinkInfo(tableName, result);
        }
    }
}
