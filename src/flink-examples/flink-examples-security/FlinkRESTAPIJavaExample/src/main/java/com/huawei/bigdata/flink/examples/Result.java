package com.huawei.bigdata.flink.examples;

import lombok.Data;

import java.util.List;

/**
 * @author psy
 * @Description
 * @Date 2025/7/15 17:19
 */
@Data
public class Result <T> {
    private int start;
    private int limit;
    private int totalCount;
    private int resultCode;
    private String resultMessage;
    private List<T> results;
}
