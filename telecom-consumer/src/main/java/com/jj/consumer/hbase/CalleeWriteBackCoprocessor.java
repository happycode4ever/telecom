package com.jj.consumer.hbase;

import com.jj.consumer.util.HBaseUtil;
import com.jj.consumer.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CalleeWriteBackCoprocessor extends BaseRegionObserver {
    private String telecomTableName;
    private int regions;
//    private String telecomTableName = "ns_telecom:calllog";
//    private int regions = Integer.parseInt("6");


    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        telecomTableName = PropertiesUtil.getProperty("telecom.table");
        regions = Integer.parseInt(PropertiesUtil.getProperty("telecom.table.regions"));
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        String currentTableName = Bytes.toString(e.getEnvironment().getRegionInfo().getTable().getName());
        //当前表与该项目无关不操作
        if (!telecomTableName.equals(currentTableName)) return;
        //05_18468618874_20180924074038_0531_18576581848_1
        String oriRow = Bytes.toString(put.getRow());
        String flag = oriRow.split("_")[5];
        //如果当前的数据是被叫数据不处理了，防止postPut方法死循环
        if ("0".equals(flag)) return;
        byte[] oriFamily = Bytes.toBytes("f1");
        byte[] oriCall1 = CellUtil.cloneValue(put.get(oriFamily, Bytes.toBytes("call1")).stream().findFirst().get());
        byte[] oriCall2 = CellUtil.cloneValue(put.get(oriFamily, Bytes.toBytes("call2")).stream().findFirst().get());
        byte[] oriBuildTime = CellUtil.cloneValue(put.get(oriFamily, Bytes.toBytes("buildTime")).stream().findFirst().get());
        byte[] oriTimeStamp = CellUtil.cloneValue(put.get(oriFamily, Bytes.toBytes("timestamp")).stream().findFirst().get());
        byte[] oriDuration = CellUtil.cloneValue(put.get(oriFamily, Bytes.toBytes("duration")).stream().findFirst().get());
        //构造新rowKey
        String call1 = Bytes.toString(oriCall2);
        String call2 = Bytes.toString(oriCall1);
        String buildTime = Bytes.toString(oriBuildTime);
        String duration = Bytes.toString(oriDuration);
        CallMsg calleeMsg = CallMsg.builder().call1(call1).call2(call2).buildTime(buildTime).duration(duration).build();
        String targetRow = HBaseUtil.genRowKey(calleeMsg, "0", regions);
        Put calleePut = new Put(Bytes.toBytes(targetRow));
        byte[] targetFamily = Bytes.toBytes("f0");
        calleePut.addColumn(targetFamily, Bytes.toBytes("call1"), oriCall2);
        calleePut.addColumn(targetFamily, Bytes.toBytes("call2"), oriCall1);
        calleePut.addColumn(targetFamily, Bytes.toBytes("buildTime"), oriBuildTime);
        calleePut.addColumn(targetFamily, Bytes.toBytes("timestamp"), oriTimeStamp);
        calleePut.addColumn(targetFamily, Bytes.toBytes("duration"), oriDuration);
        calleePut.addColumn(targetFamily, Bytes.toBytes("flag"), Bytes.toBytes("0"));
        Table table = e.getEnvironment().getTable(TableName.valueOf(telecomTableName));
        table.put(calleePut);
        table.close();
    }
}
