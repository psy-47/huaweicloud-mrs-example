package com.huawei.bigdata.flink.examples;

import lombok.Data;

/**
 * @author psy
 * @Description
 * @Date 2025/7/15 17:24
 */
@Data
public class SnapshotPath {
    private long createTime;
    private String snapshotPath;
}
