package com.alibaba.datax.plugin.reader.bishengmysqlreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum BishengMysqlReaderErrorCode implements ErrorCode {

    REQUIRED_VALUE("BishengMysqlReader-01", "您缺失了必须填写的参数值."),
    TABLE_RELATION_EMPTY("BishengMysqlReader-02", "查询到的tableRelation为空, 请先初始录入关系"),
    TABLE_META_EMPTY("BishengMysqlReader-03", "查询到的tableMeta为空"),
    SHARDING_USERPASS_NOT_SAME("BishengMysqlReader-04", "分库分表,但账号密码不相同,暂时只支持一样的账号密码,请向DBA申请"),
    NO_PARIMARY_KEY("BishengMysqlReader-05", "不存在主键"),
    REQUIRED_VALUE_INTERNAL_ERROR("BishengMysqlReader-06", "缺少Task必要的参数");

    private final String code;
    private final String description;

    private BishengMysqlReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
