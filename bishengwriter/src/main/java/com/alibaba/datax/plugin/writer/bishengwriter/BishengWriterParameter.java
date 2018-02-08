package com.alibaba.datax.plugin.writer.bishengwriter;

import java.util.List;

/**
 * @author jake.jin
 */
public class BishengWriterParameter {

    private String logicTableName;
    private List<String> column;

    public String getLogicTableName() {
        return logicTableName;
    }

    public void setLogicTableName(String logicTableName) {
        this.logicTableName = logicTableName;
    }

    public List<String> getColumn() {
        return column;
    }

    public void setColumn(List<String> column) {
        this.column = column;
    }
}
