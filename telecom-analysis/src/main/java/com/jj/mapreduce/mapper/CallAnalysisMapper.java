package com.jj.mapreduce.mapper;

import com.jj.mapreduce.dto.mapreduce.key.CombineDimension;
import com.jj.mapreduce.dto.mapreduce.key.ContactDimension;
import com.jj.mapreduce.dto.mapreduce.key.DateDimension;
import com.jj.mapreduce.dto.mapreduce.key.PhoneNameMapping;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 读取hbase采集表calllog
 * 收集到的信息有call1 call2 buildTime duration flag
 * 通话记录主要涉及3张表 后两张关联表主要为了冗余数据减少存储
 * 记录表 联系人id_时间id 通话信息 **由于要每天离线任务需要覆盖原先的结果，所以记录表需要能标识当前用户具体是哪一天的通话记录，主键采用联系人id+时间id标识
 * 关联的联系人表 联系人id 联系人信息
 * 关联的时间表 时间id 时间信息
 * keyout:组装好的维度信息
 * valueout:duration
 */
public class CallAnalysisMapper extends TableMapper<CombineDimension, IntWritable> {
    private CombineDimension combineDimension = new CombineDimension();
    private DateDimension yearDimension = new DateDimension();
    private DateDimension monthDimension = new DateDimension();
    private DateDimension dayDimension = new DateDimension();
    private ContactDimension callerDimension = new ContactDimension();
    private ContactDimension calleeDimension = new ContactDimension();
    private IntWritable durationValue = new IntWritable();
    private Logger logger = LoggerFactory.getLogger(CallAnalysisMapper.class);
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        byte[] rowKey = key.get();
        value.cellScanner();
        Cell[] cells = value.rawCells();
        //hbase 列数据
        String call1=null,call2=null,buildTime=null,duration = null;
        for(Cell cell : cells){
            //该row只筛选主叫数据
            byte[] callerFamily = Bytes.toBytes("f1");
            byte[] call1Column = Bytes.toBytes("call1");
            byte[] call2Column = Bytes.toBytes("call2");
            byte[] buildTimeColumn = Bytes.toBytes("buildTime");
            byte[] durationColumn = Bytes.toBytes("duration");
            if(CellUtil.matchingColumn(cell,callerFamily,call1Column)){
                call1 = Bytes.toString(CellUtil.cloneValue(cell));
            }
            if(CellUtil.matchingColumn(cell,callerFamily,call2Column)){
                call2 = Bytes.toString(CellUtil.cloneValue(cell));
            }
            if(CellUtil.matchingColumn(cell,callerFamily,buildTimeColumn)){
                buildTime = Bytes.toString(CellUtil.cloneValue(cell));
            }
            if(CellUtil.matchingColumn(cell,callerFamily,durationColumn)){
                duration = Bytes.toString(CellUtil.cloneValue(cell));
            }
        }
        //对Hbase的数据的ETL
        if(StringUtils.isEmpty(call1)||StringUtils.isEmpty(call2)||StringUtils.isEmpty(buildTime)||StringUtils.isEmpty(duration)){
            logger.error("rowKey:{} missing data!!!",Bytes.toString(rowKey));
            return;
        }
        //组装数据key
        //时间维度 分为整年 整月 以及细分到日
        int year = Integer.parseInt(buildTime.substring(0,4));
        int month = Integer.parseInt(buildTime.substring(4,6));
        int day = Integer.parseInt(buildTime.substring(6,8));
        yearDimension.setYear(year);
        monthDimension.setYear(year);
        monthDimension.setMonth(month);
        dayDimension.setYear(year);
        dayDimension.setMonth(month);
        dayDimension.setDay(day);
        //联系人维度
        //主叫维度
        callerDimension.setTelephone(call1);
        callerDimension.setName(PhoneNameMapping.phoneNameMap.get(call1));
        //被叫维度
        calleeDimension.setTelephone(call2);
        calleeDimension.setName(PhoneNameMapping.phoneNameMap.get(call2));
        //组装数据value
        durationValue.set(Integer.valueOf(duration));
        //主叫年月日数据 被叫年月日数据共输出6条
        combineDimension.setDateDimension(yearDimension);
        combineDimension.setContactDimension(callerDimension);
        context.write(combineDimension, durationValue);
        combineDimension.setDateDimension(monthDimension);
        combineDimension.setContactDimension(callerDimension);
        context.write(combineDimension, durationValue);
        combineDimension.setDateDimension(dayDimension);
        combineDimension.setContactDimension(callerDimension);
        context.write(combineDimension, durationValue);

        combineDimension.setDateDimension(yearDimension);
        combineDimension.setContactDimension(calleeDimension);
        context.write(combineDimension, durationValue);
        combineDimension.setDateDimension(monthDimension);
        combineDimension.setContactDimension(calleeDimension);
        context.write(combineDimension, durationValue);
        combineDimension.setDateDimension(dayDimension);
        combineDimension.setContactDimension(calleeDimension);
        context.write(combineDimension, durationValue);
    }
}
