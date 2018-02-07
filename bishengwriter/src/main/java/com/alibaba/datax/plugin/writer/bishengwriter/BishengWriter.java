package com.alibaba.datax.plugin.writer.bishengwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;
import com.weimob.tjpm.bisheng.core.dao.TableMetaOriginDao;
import com.weimob.tjpm.bisheng.core.utils.SpringBeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author jake.jin
 */
public class BishengWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private static final String SPRING_CONTEXT_PATH = "/spring/spring-context-datax-bishengwriter.xml";

        private TableMetaOriginDao tableMetaOriginDao;

        private Configuration originalConfig = null;

        @Override
        public void init() {
            originalConfig = this.getPluginJobConf();
            validateParameter();
            initServices();
        }

        private void validateParameter() {
            originalConfig.getNecessaryValue(BishengKey.LOGIC_TABLE_NAME, BishengWriterErrorCode.REQUIRED_VALUE);
        }

        private void initServices() {
            if (null == tableMetaOriginDao) {
                tableMetaOriginDao = SpringBeanUtils.getBean(TableMetaOriginDao.class, SPRING_CONTEXT_PATH);
            }
        }

        @Override
        public void prepare() {
            String logicTableName = originalConfig.getString(BishengKey.LOGIC_TABLE_NAME);
            //TODO: 调用service, 字段顺序
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

        @Override
        public void init() {
            writerSliceConfig = this.getPluginJobConf();
            String logicTableName = writerSliceConfig.getString(BishengKey.LOGIC_TABLE_NAME);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("begin do write...");
            lineReceiver.getFromReader().getColumn(0);
            LOG.info("end do write");
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }
}
