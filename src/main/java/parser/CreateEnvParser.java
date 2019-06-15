package parser;

import Util.SplitSql;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//解析创建环境语句

public class CreateEnvParser implements IParser{
    private static final String PATTERN_STR = "create\\s+env\\s+(\\S+)\\s*\\((.+)\\)\\s*WITH\\s*\\((.+)\\)";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    public static CreateEnvParser newInstance() {
        return new CreateEnvParser();
    }

    public boolean verify(String sql) {
        return PATTERN.matcher(sql).find();
    }

    public void parseSql(String sql, SqlTree sqlTree) {
        Matcher matcher = PATTERN.matcher(sql);
        if (matcher.find()) {
            String sparkEnv = matcher.group(2);
            String appConf = matcher.group(3);
            Map<String, Object> props = SplitSql.parseProp(sparkEnv);
            sqlTree.addPreDealSparkEnvInfo(props);
            sqlTree.setAppInfo(SplitSql.getAppName(appConf));
        }
    }
}
