package com.alibaba.datax.plugin.writer.bishengwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author jake.jin
 */
public enum BishengWriterErrorCode implements ErrorCode {
    
    CONFIG_INVALID_EXCEPTION("BishengWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("BishengWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("BishengWriter-02", "您填写的参数值不合法."),
    TABLE_META_EMPTY("BishengWriter-03", "查询到的tableMeta为空"),
    Write_FILE_IO_ERROR("BishengWriter-04", "您配置的文件在写入时出现IO异常."),
    SECURITY_NOT_ENOUGH("BishengWriter-05", "您缺少权限执行相应的文件写入操作."),
    RUNTIME_EXCEPTION("BishengWriter-06", "运行时异常"),
    REQUIRED_VALUE_INTERNAL_ERROR("BishengWriter-07", "缺少Task必要的参数");

    private final String code;
    private final String description;

    private BishengWriterErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}
