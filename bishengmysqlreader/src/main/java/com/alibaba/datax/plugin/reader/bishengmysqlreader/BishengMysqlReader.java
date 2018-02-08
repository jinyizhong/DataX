package com.alibaba.datax.plugin.reader.bishengmysqlreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.weimob.tjpm.bisheng.core.model.JDBCConnect;
import com.weimob.tjpm.bisheng.core.model.TableMetaOrigin;
import com.weimob.tjpm.bisheng.core.model.TableRelation;
import com.weimob.tjpm.bisheng.core.service.SafeService;
import com.weimob.tjpm.bisheng.core.service.TableMetaOriginService;
import com.weimob.tjpm.bisheng.core.service.TableRelationService;
import com.weimob.tjpm.bisheng.core.utils.CollectionUtils;
import com.weimob.tjpm.bisheng.core.utils.SpringBeanUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BishengMysqlReader extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.MySql;
    private static final String SPRING_CONTEXT_PATH = "/spring/spring-context-datax-bishengmysqlreader.xml";

    private static <T> T getBean(Class<T> clazz) {
        return SpringBeanUtils.getBean(clazz, SPRING_CONTEXT_PATH);
    }

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private BishengRdbmsReader.Job bishengRdbmsReaderJob;
        private Configuration originalConfig = null;

        private TableRelationService tableRelationService;
        private TableMetaOriginService tableMetaOriginService;
        private SafeService safeService;

        @Override
        public void init() {
            originalConfig = super.getPluginJobConf();
            validateParameter();
            initServices();
            fillInConfigs();
        }

        private void validateParameter() {
            originalConfig.getNecessaryValue(BishengKey.LOGIC_TABLE_NAME, BishengMysqlReaderErrorCode.REQUIRED_VALUE);
        }

        private void initServices() {
            LOG.info("Init bisheng services");
            if (null == tableRelationService) {
                tableRelationService = getBean(TableRelationService.class);
            }
            if (null == tableMetaOriginService) {
                tableMetaOriginService = getBean(TableMetaOriginService.class);
            }
            if (null == safeService) {
                safeService = getBean(SafeService.class);
            }
        }

        private void fillInConfigs() {
            LOG.info("Config before fill in: {}", originalConfig);
            // 入参
            String logicTableName = originalConfig.getString(BishengKey.LOGIC_TABLE_NAME);
            BishengMysqlParameter outputParameter = new BishengMysqlParameter();
            outputParameter.setLogicTableName(logicTableName);

            // 根据logicTableName查TableRelation
            LOG.info("Get table relations from bisheng service");
            List<TableRelation> tableRelations = tableRelationService.getTableRelationsByLogicTableName(logicTableName);
            if (CollectionUtils.isEmpty(tableRelations)) {
                throw DataXException.asDataXException(
                        BishengMysqlReaderErrorCode.TABLE_RELATION_EMPTY,
                        "logicTableName: [" + logicTableName + "]");
            }

            LOG.info("Start fill in configs");
            // 不同inst, 不同db, 将拆分到各个connection中
            Map<Pair<String, String>, List<String>> tableGroupMap = Maps.newHashMap();
            Map<String, Integer> partitionMap = Maps.newHashMap();
            for (TableRelation tableRelation : tableRelations) {
                String inst = tableRelation.getInstance();
                String db = tableRelation.getTableSchema();
                String table = tableRelation.getTableName();
                Integer partition = tableRelation.getPartition();

                // table分组
                Pair<String, String> instDb = Pair.of(inst, db);
                if (!tableGroupMap.containsKey(instDb)) {
                    tableGroupMap.put(instDb, Lists.<String>newArrayList());
                }
                tableGroupMap.get(instDb).add(table);

                // 构建partition配置传参
                String partKey = Joiner.on(BishengConstant.BISHENG_SPLITTER).join(Lists.newArrayList(inst, db, table));
                partitionMap.put(partKey, partition);
            }

            String targetUsername = null;
            String targetPassword = null;
            Set<String> instSet = Sets.newHashSet();
            List<BishengMysqlParameter.Conn> outputConns = Lists.newArrayList();
            for (Map.Entry<Pair<String, String>, List<String>> entry : tableGroupMap.entrySet()) {
                Pair<String, String> instDb = entry.getKey();
                List<String> tables = entry.getValue();
                instSet.add(instDb.getLeft());
                JDBCConnect jdbcConnect = safeService.getDatabaseConnectInfo(instDb.getLeft(), instDb.getRight());
                // 分实例只支持账号密码相同, 做验证, 不相同就报错
                if (null == targetUsername || null == targetPassword) {
                    targetUsername = jdbcConnect.getUsername();
                    targetPassword = jdbcConnect.getPassword();
                    outputParameter.setUsername(targetUsername);
                    outputParameter.setPassword(targetPassword);
                } else {
                    if (!targetUsername.equals(jdbcConnect.getUsername()) || !targetPassword.equals(jdbcConnect.getPassword())) {
                        throw DataXException.asDataXException(
                                BishengMysqlReaderErrorCode.SHARDING_USERPASS_NOT_SAME,
                                "logicTableName: [" + logicTableName + "]");
                    }
                }

                BishengMysqlParameter.Conn conn = new BishengMysqlParameter.Conn();
                conn.setTable(tables);
                conn.setJdbcUrl(Lists.newArrayList(jdbcConnect.getUrl()));
                conn.setInstance(instDb.getLeft());
                conn.setTableSchema(instDb.getRight());
                conn.setTableName("");  // tableName将在task中展开
                conn.setPartition(partitionMap);
                outputConns.add(conn);
            }
            outputParameter.setConnection(outputConns);

            // 更新meta信息
            for (String inst : instSet) {
                tableMetaOriginService.updateMeta(inst);
            }

            // 配置字段, 只取一个, 默认结构都一样
            List<TableMetaOrigin> tableMetas = tableMetaOriginService.getTableMetaOrigin(logicTableName);
            if (CollectionUtils.isEmpty(tableMetas)) {
                throw DataXException.asDataXException(
                        BishengMysqlReaderErrorCode.TABLE_META_EMPTY,
                        "logicTableName: [" + logicTableName + "]");
            }
            List<String> columns = Lists.newArrayList();
            String pk = null;
            for (TableMetaOrigin tableMeta : tableMetas) {
                columns.add(tableMeta.getColumnName());
                if (null == pk && tableMeta.getIsKey()) {
                    pk = tableMeta.getColumnName();
                }
            }
            if (null == pk) {
                throw DataXException.asDataXException(
                        BishengMysqlReaderErrorCode.NO_PARIMARY_KEY,
                        BishengMysqlReaderErrorCode.NO_PARIMARY_KEY.getDescription());
            }

            outputParameter.setColumn(columns);
            outputParameter.setSplitPk(pk);

            // 这里必须转成json填入, 模拟正常的json文件输入, 否则会导致后续类型转换错误
            originalConfig = Configuration.from(JSONObject.toJSONString(outputParameter));

            // 其他配置
            originalConfig.set(Constant.FETCH_SIZE, Integer.MIN_VALUE);

            LOG.info("Config after fill in: {}", originalConfig);

            bishengRdbmsReaderJob = new BishengRdbmsReader.Job(DATABASE_TYPE);
            bishengRdbmsReaderJob.init(originalConfig);
        }

        @Override
        public void preCheck() {
            // init();
            bishengRdbmsReaderJob.preCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return bishengRdbmsReaderJob.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            bishengRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            bishengRdbmsReaderJob.destroy(this.originalConfig);
        }

    }

    /**
     * 和原版mysqlreader不同之处:
     * bisheng需要的 instance, tableSchema, tableName, partition 会参数填在record头部, 约定好顺序
     */
    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private BishengRdbmsReader.Task bishengRdbmsReaderTask;

        @Override
        public void init() {
            readerSliceConfig = super.getPluginJobConf();
            bishengRdbmsReaderTask = new BishengRdbmsReader.Task(DATABASE_TYPE, super.getTaskGroupId(), super.getTaskId());
            bishengRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = readerSliceConfig.getInt(Constant.FETCH_SIZE);
            bishengRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.bishengRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.bishengRdbmsReaderTask.destroy(this.readerSliceConfig);
        }

    }

}
