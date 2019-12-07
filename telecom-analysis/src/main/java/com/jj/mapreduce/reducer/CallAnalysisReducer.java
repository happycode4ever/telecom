package com.jj.mapreduce.reducer;

import com.jj.mapreduce.dto.mapreduce.key.CombineDimension;
import com.jj.mapreduce.dto.mapreduce.value.AnalysisResult;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * reducer输入就是mapper输出维度和时长
 * reducer输出给到ouputFormat的recordWriter需要维度数据以及分析后的结果数据
 * 同一个主叫或者被叫的分别的年月日维度数据作为一组进到reduce汇总
 */
public class CallAnalysisReducer extends Reducer<CombineDimension, IntWritable, CombineDimension, AnalysisResult> {
    private AnalysisResult analysisResult = new AnalysisResult();
    private Logger logger = LoggerFactory.getLogger(CallAnalysisReducer.class);
    @Override
    protected void reduce(CombineDimension key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int callSum = 0;
        int durationSum = 0;
        for (IntWritable duration : values) {
            durationSum += duration.get();
            callSum++;
        }
        analysisResult.setCallSum(callSum);
        analysisResult.setDurationSum(durationSum);
        context.write(key, analysisResult);
    }
}
