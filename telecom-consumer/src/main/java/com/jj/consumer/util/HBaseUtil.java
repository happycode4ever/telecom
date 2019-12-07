package com.jj.consumer.util;

import com.google.common.collect.Lists;
import com.jj.consumer.hbase.CallMsg;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class HBaseUtil {
    private static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
    }

    public static Connection getConnection() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    public static boolean tableExists(String tableName) throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        boolean res = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        connection.close();
        return res;
    }

    public static boolean namespaceExists(String namespace) throws IOException {
        if(StringUtils.isEmpty(namespace)){
            throw new IOException("namespace empty!!!!");
        }
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        List<String> namespaces = Lists.newArrayList();
        NamespaceDescriptor[] nsds = admin.listNamespaceDescriptors();
        //校验命名空间
        for (NamespaceDescriptor nsd : nsds) {
            String name = nsd.getName();
            namespaces.add(name);
        }
        if(namespaces.contains(namespace)){
            return true;
        }
        return false;
    }

    public static void createTable(String tableName, String... family) throws IOException {
        createTable(-1,null, tableName,null,null, family);
    }

    public static void createTable(int regions, String namespace, String tableName,String jarFilePath,Class processorClass, String... family) throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        List<String> namespaces = Lists.newArrayList();

        //不存在命名空间才创建
        if (!namespaceExists(namespace)) {
            NamespaceDescriptor newnsd = NamespaceDescriptor
                    .create(namespace)
                    .addConfiguration("creator", "jj")
                    .addConfiguration("createTime", LocalDate.now().toString())
                    .build();
            admin.createNamespace(newnsd);
        }
        //表不存在才创建
        if (!tableExists(tableName)) {
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
            if(Objects.nonNull(processorClass)){
                if(StringUtils.isEmpty(jarFilePath)){
                    throw new IOException(processorClass.getCanonicalName()+" jarFilePath is Empty!!!!");
                }
                //**动态加载协处理器 注意协处理器加载不要让regionserver挂掉
                htd.addCoprocessor(processorClass.getCanonicalName(),new Path(jarFilePath),Coprocessor.PRIORITY_USER,null);
//                htd.addCoprocessor(processorClass.getCanonicalName());
            }
            for (String cf : family) {
                htd.addFamily(new HColumnDescriptor(cf));
            }
            //**指定分区键
            if (regions > 0) {
                admin.createTable(htd, genSplitKeys(regions));
            } else {
                admin.createTable(htd);
            }
            admin.close();
            connection.close();
        }
    }

    /**
     * 根据指定分区数生成分区键|在ascii值是124，所以生成的键最右一个分区永远不会有数据
     * @param regions
     * @return
     */
    private static byte[][] genSplitKeys(int regions) {
        byte[][] keyByteArr = new byte[regions][];
        DecimalFormat df = new DecimalFormat("00");
        //创建分区 00|,01|,02|...
        for (int i = 0; i < regions; i++) {
            keyByteArr[i] = Bytes.toBytes(df.format(i) + "|");
        }
        //分区键一定要有序，使用Hbase自带的比较器
        Arrays.sort(keyByteArr, Bytes.BYTES_COMPARATOR);
        //输出结果测试
//        for(byte[] ba : keyByteArr){
//            System.out.println(Arrays.toString(ba));
//        }
        return keyByteArr;
    }

    /***
     * 获取rowKey格式样例
     * 01_19212344321_20180101111050_0360_13433312345_1
     * region_call1_buildTime_duration_call2_flag
     * flag：1表示call1主叫 call2被叫 0表示call1被叫 call2主叫（该部分由协处理器完成，原始数据均是主叫）
     * @param callMsg 原始数据
     * @param regions 分区数
     * @return
     */
    public static String genRowKey(CallMsg callMsg,String flag, int regions){
        String call1 = callMsg.getCall1();
        String call2 = callMsg.getCall2();
        String buildTime = callMsg.getBuildTime();
        String duration = callMsg.getDuration();
        //格式化时间处理
        if(StringUtils.isEmpty(buildTime)||buildTime.length()<6){
            System.out.println(callMsg + " genError!!!");
            return null;
        }
        buildTime = buildTime.
                replaceAll("-", "")
                .replaceAll(":", "")
                .replaceAll(" ","");
        DecimalFormat df = new DecimalFormat("0000");
        String durationFormat = df.format(Integer.parseInt(duration));
        //**获取region
        String regionCode = genRegionCode(call1,buildTime,regions);
       //拼接结果
        StringBuffer sb = new StringBuffer();
        sb.append(regionCode + "_")
                .append(call1 + "_")
                .append(buildTime+"_")
                .append(durationFormat+"_")
                .append(call2+"_")
                .append(flag);
        return sb.toString();
    }

    /**
     * 生成分区号，要保证当前手机号在同一年的同月份生成的分区号相同
     * @param call1
     * @param buildTime
     * @return
     */
    private static String genRegionCode(String call1, String buildTime, int regions) {
        //时间取到月级别，如201810
        String buildTimeFormat = buildTime.substring(0, 6);
        //离散1
        Long i1 = Long.parseLong(call1) ^ Long.parseLong(buildTimeFormat);
        //离散2注意hashcode可能是负数
        int i2 = i1.hashCode()&Integer.MAX_VALUE;
        //离散3保证结果落在分区内
        int i3 = i2 % regions;
        String regionCode = new DecimalFormat("00").format(i3);
        return regionCode;
    }

//    public static void main(String[] args) {
//        CallMsg callMsg = CallMsg.builder().call1("135353123444").call2("13068733333").buildTime("2019-08-31 15:00:32").duration("66").build();
//        String rowKey = genRowKey(callMsg, 6);
//        System.out.println(rowKey);
//    }
}
