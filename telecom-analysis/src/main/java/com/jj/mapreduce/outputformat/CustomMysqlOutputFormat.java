package com.jj.mapreduce.outputformat;

import com.jj.mapreduce.dto.mapreduce.key.CombineDimension;
import com.jj.mapreduce.dto.mapreduce.value.AnalysisResult;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

public class CustomMysqlOutputFormat extends OutputFormat<CombineDimension, AnalysisResult> {
    @Override
    public RecordWriter<CombineDimension, AnalysisResult> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new CustomMysqlRecordWriter(taskAttemptContext);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    //拷贝自FileOutputFormat
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        String name = taskAttemptContext.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
        Path output = name == null ? null : new Path(name);
        return new FileOutputCommitter(output, taskAttemptContext);
    }
}
