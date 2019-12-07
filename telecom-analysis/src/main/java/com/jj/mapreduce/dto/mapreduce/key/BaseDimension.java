package com.jj.mapreduce.dto.mapreduce.key;

import org.apache.hadoop.io.WritableComparable;

/**
 * 维度抽象类 主要是时间维度和联系人维度
 */
public abstract class BaseDimension implements WritableComparable<BaseDimension> {
}
