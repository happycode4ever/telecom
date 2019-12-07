package com.jj.consumer.kafka;

import com.google.common.collect.Lists;
import com.jj.consumer.hbase.HBaseDAO;
import com.jj.consumer.util.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.List;

public class FlumeToKafkaConsumer{
    public static void main(String[] args) {
        HBaseDAO dao = new HBaseDAO();
        //读取配置文件构建consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(PropertiesUtil.prop);
        //订阅配置里的主题
        consumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("comsume.topic")));
        while(true){
            //每100ms拉取一块消息
            List<String> values = Lists.newArrayList();
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for(ConsumerRecord<String,String> record : consumerRecords){
                System.out.println("----------consume records------------");
                System.out.println(record.value());
                values.add(record.value());
            }
            dao.putCallMsg(values);
        }
    }
}
