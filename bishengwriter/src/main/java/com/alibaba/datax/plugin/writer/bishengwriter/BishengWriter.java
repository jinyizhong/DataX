package com.alibaba.datax.plugin.writer.bishengwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weimob.tjpm.bisheng.core.model.TableMetaOrigin;
import com.weimob.tjpm.bisheng.core.service.TableMetaOriginService;
import com.weimob.tjpm.bisheng.core.utils.CollectionUtils;
import com.weimob.tjpm.bisheng.core.utils.SpringBeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author jake.jin
 */
public class BishengWriter extends Writer {

    private static final String SPRING_CONTEXT_PATH = "/spring/spring-context-datax-bishengwriter.xml";

    private static <T> T getBean(Class<T> clazz) {
        return SpringBeanUtils.getBean(clazz, SPRING_CONTEXT_PATH);
    }

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;
        private String logicTableName;
        private TableMetaOriginService tableMetaOriginService;

        @Override
        public void init() {
            originalConfig = this.getPluginJobConf();
            validateParameter();
            logicTableName = originalConfig.getString(BishengKey.LOGIC_TABLE_NAME);
            initServices();
            fillInParams();
        }

        private void validateParameter() {
            originalConfig.getNecessaryValue(BishengKey.LOGIC_TABLE_NAME, BishengWriterErrorCode.REQUIRED_VALUE);
        }

        private void initServices() {
            if (null == tableMetaOriginService) {
                tableMetaOriginService = getBean(TableMetaOriginService.class);
            }
        }

        private void fillInParams() {
            LOG.info("Config before fill in: {}", originalConfig);

            BishengWriterParameter outputParameter = new BishengWriterParameter();
            outputParameter.setLogicTableName(logicTableName);

            // 配置字段, 只取一个, 默认结构都一样
            List<TableMetaOrigin> tableMetas = tableMetaOriginService.getTableMetaOrigin(logicTableName);
            if (CollectionUtils.isEmpty(tableMetas)) {
                throw DataXException.asDataXException(
                        BishengWriterErrorCode.TABLE_META_EMPTY,
                        "logicTableName: [" + logicTableName + "]");
            }

            List<String> columns = Lists.newArrayList();
            for (TableMetaOrigin tableMeta : tableMetas) {
                columns.add(tableMeta.getColumnName());
            }
            outputParameter.setColumn(columns);

            originalConfig = Configuration.from(JSONObject.toJSONString(outputParameter));

            LOG.info("Config after fill in: {}", originalConfig);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            List<Configuration> writerSliceConfigs = Lists.newArrayList();
            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration writerSliceConfig = originalConfig.clone();
                writerSliceConfigs.add(writerSliceConfig);
            }
            LOG.info("end do split.");
            return writerSliceConfigs;
        }
    }


    public static class Task extends Writer.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;
        private String logicTableName;
        private List<String> columnNames;

        @Override
        public void init() {
            writerSliceConfig = this.getPluginJobConf();
            LOG.debug("task config: {}", writerSliceConfig);
            validateBishengParameter(writerSliceConfig);
            logicTableName = writerSliceConfig.getString(BishengKey.LOGIC_TABLE_NAME);
            columnNames = writerSliceConfig.getList(BishengKey.COLUMN, String.class);
        }


        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            LOG.info("begin do write...");

            try {
                BishengDTO dto = new BishengDTO();
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    int recordLength = record.getColumnNumber();
                    if (0 == recordLength) {
                        continue;
                    }
                    Column instanceCol = record.getColumn(0);
                    dto.setInstance(instanceCol.asString());
                    Column tableSchemaCol = record.getColumn(1);
                    dto.setTableSchema(tableSchemaCol.asString());
                    Column tableNameCol = record.getColumn(2);
                    dto.setTableName(tableNameCol.asString());
                    Column partitionCol = record.getColumn(3);
                    dto.setPartition(partitionCol.asLong().intValue());

                    Map<String, String> data = Maps.newHashMap();
                    for (int i = 4; i < recordLength; i++) {
                        String columnName = columnNames.get(i - 4);
                        String columnValue = record.getColumn(i).asString();
                        data.put(columnName, columnValue);
                    }
                    dto.setData(data);
                    dto.setEvent("UPDATE");

                    LOG.info("{} BishengDTO: {}", logicTableName, dto);
                }
                // TODO 批量处理
            } catch (Exception e) {
                throw DataXException.asDataXException(BishengWriterErrorCode.RUNTIME_EXCEPTION, e);
            }

            LOG.info("end do write");
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        private void validateBishengParameter(Configuration config) {
            config.getNecessaryValue(BishengKey.LOGIC_TABLE_NAME, BishengWriterErrorCode.REQUIRED_VALUE_INTERNAL_ERROR);
            config.getNecessaryValue(BishengKey.COLUMN, BishengWriterErrorCode.REQUIRED_VALUE_INTERNAL_ERROR);
        }
    }

}
