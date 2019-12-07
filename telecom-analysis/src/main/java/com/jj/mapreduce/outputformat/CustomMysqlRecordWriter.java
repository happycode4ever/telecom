package com.jj.mapreduce.outputformat;

import com.jj.mapreduce.dto.mapreduce.key.CombineDimension;
import com.jj.mapreduce.dto.mapreduce.key.ContactDimension;
import com.jj.mapreduce.dto.mapreduce.key.DateDimension;
import com.jj.mapreduce.dto.mapreduce.value.AnalysisResult;
import com.jj.mapreduce.util.JDBCUtil;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.Connection;

public class CustomMysqlRecordWriter extends RecordWriter<CombineDimension, AnalysisResult> {
    private WritebackMysqlService service = new WritebackMysqlService();


    public CustomMysqlRecordWriter(TaskAttemptContext taskAttemptContext) {
    }

    @Override
    public void write(CombineDimension combineDimension, AnalysisResult analysisResult) throws IOException, InterruptedException {
        service.writeMysql(combineDimension,analysisResult);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }
}
