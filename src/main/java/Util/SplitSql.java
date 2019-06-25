package Util;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import javax.annotation.Nullable;
import java.util.*;

public class SplitSql {
    //将语句按传进来的符号分成若干句
    public static List<String> splitWithDel(String str, char delimiter) {
        List<String> tokensList = new ArrayList<>();
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        StringBuilder b = new StringBuilder();
        for (char c : str.toCharArray()) {
            if (c == delimiter) {
                if (inQuotes) {
                    b.append(c);
                } else if (inSingleQuotes) {
                    b.append(c);
                } else {
                    tokensList.add(b.toString());
                    b = new StringBuilder();
                }
            } else if (c == '\"') {
                inQuotes = !inQuotes;
                b.append(c);
            } else if (c == '\'') {
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
            } else {
                b.append(c);
            }
        }

        tokensList.add(b.toString());

        return tokensList;
    }
    //按等于号分为键值对
    public static Map parseProp(String propsStr) {
        String[] strs = propsStr.trim().split("'\\s*,");
        Map<String, Object> propMap = new HashMap<>();
        for (int i = 0; i < strs.length; i++) {
            List<String> ss = SplitSql.splitWithDel(strs[i], '=');
            String key = ss.get(0).trim();
            String value = ss.get(1).trim().replaceAll("'", "").trim();
            propMap.put(key, value);
        }

        return propMap;
    }

    //获取appName
    public static String getAppName(String appInfo){
        List<String> ss = SplitSql.splitWithDel(appInfo.trim(), '=');
        String appName = ss.get(1).trim().replaceAll("'", "").trim();
        return appName;
    }

    //创建List和Set
    public static <E> HashSet<E> newHashSet(E... elements) {
        checkNotNull(elements);
        HashSet<E> list = new HashSet(elements.length);
        Collections.addAll(list, elements);
        return list;
    }

    public static <E> List<E> newArrayList(E... elements) {
        checkNotNull(elements);
        List<E> list = new ArrayList<>(elements.length);
        Collections.addAll(list, elements);
        return list;
    }

    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        } else {
            return reference;
        }
    }

    public static boolean isNullOrEmpty(@Nullable String string) {
        return string == null || string.length() == 0;
    }
    public static String upperCaseFirstChar(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    public static String[] splitIgnoreQuotaBrackets(String str, String delimter) {
        String splitPatternStr = delimter + "(?![^()]*+\\))(?![^{}]*+})(?![^\\[\\]]*+\\])(?=(?:[^\"]|\"[^\"]*\")*$)";
        return str.split(splitPatternStr);
    }

    public static DataType strConverDataType(String filedType) {
        switch (filedType) {
            case "boolean":
                return DataTypes.BooleanType;
            case "int":
                //2019.4.26
            case "video":
                return DataTypes.IntegerType;
            case "bigint":
            case "int64":
                return DataTypes.LongType;

            case "tinyint":
            case "byte":
                return DataTypes.ByteType;

            case "short":
            case "smallint":
                return DataTypes.ShortType;

            case "char":
            case "varchar":
            case "string":
                return DataTypes.StringType;

            case "float":
                return DataTypes.FloatType;

            case "double":
                return DataTypes.DoubleType;

            case "date":
                return DataTypes.DateType;

            case "timestamp":
                return DataTypes.TimestampType;

        }

        throw new RuntimeException("不支持 " + filedType + " 类型");
    }




}
