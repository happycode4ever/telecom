package com.jj.mapreduce.driver;

import com.jj.mapreduce.dto.mapreduce.key.CombineDimension;
import com.jj.mapreduce.dto.mapreduce.value.AnalysisResult;
import com.jj.mapreduce.mapper.CallAnalysisMapper;
import com.jj.mapreduce.reducer.CallAnalysisReducer;
import com.jj.mapreduce.outputformat.CustomMysqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CallAnalysisDriver implements Tool {
    private Configuration configuration;
    private String tableName;

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new CallAnalysisDriver(), args);
        System.exit(run);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(CallAnalysisDriver.class);
        tableName = "ns_telecom:calllog";
        Scan scan = new Scan();
        //**可以指定多个scan通过设定属性添加表名
//        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("tableName"));
        TableMapReduceUtil.initTableMapperJob(tableName, scan,CallAnalysisMapper.class, CombineDimension.class, IntWritable.class,job);
        job.setReducerClass(CallAnalysisReducer.class);
        job.setOutputKeyClass(CombineDimension.class);
        job.setOutputValueClass(AnalysisResult.class);
        job.setOutputFormatClass(CustomMysqlOutputFormat.class);
        return job.waitForCompletion(true) ? 1 : -1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = HBaseConfiguration.create(configuration);

    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }
}
