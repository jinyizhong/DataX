package com.alibaba.datax.plugin.writer.bishengwriter;

import java.util.Map;

/**
 * @author jake.jin
 */
public class BishengDTO {

    private String instance;
    private String tableSchema;
    private String tableName;
    private Integer partition;
    private String event;
    private Map<String, String> data;

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("BishengDTO{");
        sb.append("instance='").append(instance).append('\'');
        sb.append(", tableSchema='").append(tableSchema).append('\'');
        sb.append(", tableName='").append(tableName).append('\'');
        sb.append(", partition=").append(partition);
        sb.append(", event='").append(event).append('\'');
        sb.append(", data=").append(data);
        sb.append('}');
        return sb.toString();
    }
}
