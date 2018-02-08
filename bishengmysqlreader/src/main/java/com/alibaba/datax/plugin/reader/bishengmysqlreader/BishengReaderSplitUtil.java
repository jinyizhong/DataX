package com.alibaba.datax.plugin.reader.bishengmysqlreader;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.reader.util.HintUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * bisheng扩充schema字段
 *
 * @author jake
 */
public final class BishengReaderSplitUtil {
    private static final Logger LOG = LoggerFactory.getLogger(BishengReaderSplitUtil.class);

    public static List<Configuration> doSplit(Configuration originalSliceConfig, int adviceNumber) {
        boolean isTableMode = originalSliceConfig.getBool(Constant.IS_TABLE_MODE);
        int eachTableShouldSplittedNumber = -1;
        if (isTableMode) {
            // adviceNumber这里是channel数量大小, 即datax并发task数量
            // eachTableShouldSplittedNumber是单表应该切分的份数, 向上取整可能和adviceNumber没有比例关系了已经
            eachTableShouldSplittedNumber = calculateEachTableShouldSplittedNumber(
                    adviceNumber, originalSliceConfig.getInt(Constant.TABLE_NUMBER_MARK));
        }

        String column = originalSliceConfig.getString(Key.COLUMN);
        String where = originalSliceConfig.getString(Key.WHERE, null);

        List<Object> conns = originalSliceConfig.getList(Constant.CONN_MARK, Object.class);

        List<Configuration> splittedConfigs = new ArrayList<Configuration>();

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration sliceConfig = originalSliceConfig.clone();

            Configuration connConf = Configuration.from(conns.get(i).toString());
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

            // bisheng补充 instance tableSchema
            // modify by jake.jin @2018-02-06
            String bishengInstance = connConf.getString(BishengConstant.BISHENG_INSTANCE);
            if (null != bishengInstance) {
                sliceConfig.set(BishengConstant.BISHENG_INSTANCE, bishengInstance);
            }
            String bishengTableSchema = connConf.getString(BishengConstant.BISHENG_TABLE_SCHEMA);
            if (null != bishengTableSchema) {
                sliceConfig.set(BishengConstant.BISHENG_TABLE_SCHEMA, bishengTableSchema);
            }
            Map<String, Object> partitionMap = connConf.getMap(BishengConstant.BISHENG_TABLE_PARTITION);

            // 抽取 jdbcUrl 中的 ip/port 进行资源使用的打标，以提供给 core 做有意义的 shuffle 操作
            sliceConfig.set(CommonConstant.LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));

            sliceConfig.remove(Constant.CONN_MARK);

            Configuration tempSlice;

            // 说明是配置的 table 方式
            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(Key.TABLE, String.class);

                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");

                String splitPk = originalSliceConfig.getString(Key.SPLIT_PK, null);

                //最终切分份数不一定等于 eachTableShouldSplittedNumber
                boolean needSplitTable = eachTableShouldSplittedNumber > 1
                        && StringUtils.isNotBlank(splitPk);
                if (needSplitTable) {
                    if (tables.size() == 1) {
                        //原来:如果是单表的，主键切分num=num*2+1
                        // splitPk is null这类的情况的数据量本身就比真实数据量少很多, 和channel大小比率关系时，不建议考虑
                        //eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * 2 + 1;// 不应该加1导致长尾

                        //考虑其他比率数字?(splitPk is null, 忽略此长尾)
                        eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * 5;
                    }
                    // 尝试对每个表，切分为eachTableShouldSplittedNumber 份
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);
                        tempSlice.set(BishengConstant.BISHENG_TABLE_NAME, table);

                        // bisheng补充: 增加分区
                        if (null != partitionMap && null != bishengInstance && null != bishengTableSchema) {
                            Object partition = partitionMap.get(
                                    Joiner.on(BishengConstant.BISHENG_SPLITTER).join(Lists.newArrayList(bishengInstance, bishengTableSchema, table))
                            );
                            if (null != partition) {
                                tempSlice.set(BishengConstant.BISHENG_TABLE_PARTITION, partition);
                            }
                        }

                        List<Configuration> splittedSlices = SingleTableSplitUtil
                                .splitSingleTable(tempSlice, eachTableShouldSplittedNumber);

                        splittedConfigs.addAll(splittedSlices);
                    }
                } else {
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);
                        tempSlice.set(BishengConstant.BISHENG_TABLE_NAME, table);

                        // bisheng补充: 增加分区
                        if (null != partitionMap && null != bishengInstance && null != bishengTableSchema) {
                            Object partition = partitionMap.get(
                                    Joiner.on(BishengConstant.BISHENG_SPLITTER).join(Lists.newArrayList(bishengInstance, bishengTableSchema, table))
                            );
                            if (null != partition) {
                                tempSlice.set(BishengConstant.BISHENG_TABLE_PARTITION, partition);
                            }
                        }

                        String queryColumn = HintUtil.buildQueryColumn(jdbcUrl, table, column);
                        tempSlice.set(Key.QUERY_SQL, SingleTableSplitUtil.buildQuerySql(queryColumn, table, where));
                        splittedConfigs.add(tempSlice);
                    }
                }
            } else {
                // 说明是配置的 querySql 方式
                List<String> sqls = connConf.getList(Key.QUERY_SQL, String.class);

                // TODO 是否check 配置为多条语句？？
                for (String querySql : sqls) {
                    tempSlice = sliceConfig.clone();
                    tempSlice.set(Key.QUERY_SQL, querySql);
                    splittedConfigs.add(tempSlice);
                }
            }
        }

        LOG.info("Configs after bisheng reader split: {}", splittedConfigs);

        return splittedConfigs;
    }

    public static Configuration doPreCheckSplit(Configuration originalSliceConfig) {
        return ReaderSplitUtil.doPreCheckSplit(originalSliceConfig);
    }

    private static int calculateEachTableShouldSplittedNumber(int adviceNumber, int tableNumber) {
        double tempNum = 1.0 * adviceNumber / tableNumber;
        return (int) Math.ceil(tempNum);
    }

}
