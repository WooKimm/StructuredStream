package Util;

import java.io.Serializable;

public class ColumnType implements Serializable {
    transient private String name;
    transient private String attribute;

    public ColumnType(String name, String attribute)
    {
        this.name = name;
        this.attribute = attribute;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }
}

