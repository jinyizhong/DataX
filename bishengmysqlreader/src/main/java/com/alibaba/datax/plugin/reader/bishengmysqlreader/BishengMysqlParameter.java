package com.alibaba.datax.plugin.reader.bishengmysqlreader;

import java.util.List;
import java.util.Map;

/**
 * @author jake.jin
 */
public class BishengMysqlParameter {

    private String username;
    private String password;
    private String splitPk;
    private List<String> column;
    private List<Conn> connection;
    private String logicTableName;

    public String getLogicTableName() {
        return logicTableName;
    }

    public void setLogicTableName(String logicTableName) {
        this.logicTableName = logicTableName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSplitPk() {
        return splitPk;
    }

    public void setSplitPk(String splitPk) {
        this.splitPk = splitPk;
    }

    public List<String> getColumn() {
        return column;
    }

    public void setColumn(List<String> column) {
        this.column = column;
    }

    public List<Conn> getConnection() {
        return connection;
    }

    public void setConnection(List<Conn> connection) {
        this.connection = connection;
    }

    public static class Conn {
        private List<String> table;
        private List<String> jdbcUrl;
        private String instance;
        private String tableSchema;
        private String tableName;
        private Map<String, Integer> partition;

        public List<String> getTable() {
            return table;
        }

        public void setTable(List<String> table) {
            this.table = table;
        }

        public List<String> getJdbcUrl() {
            return jdbcUrl;
        }

        public void setJdbcUrl(List<String> jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

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

        public Map<String, Integer> getPartition() {
            return partition;
        }

        public void setPartition(Map<String, Integer> partition) {
            this.partition = partition;
        }
    }

}
