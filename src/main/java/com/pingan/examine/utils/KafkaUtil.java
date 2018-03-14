package com.pingan.examine.utils;

import com.pingan.examine.start.ConfigFactory;
import jodd.util.StringUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Administrator on 2017/12/15.
 */
public class KafkaUtil {
    private static KafkaProducer<String,String> producer = null;

    /**
     * 发送消息到Kafka
     * @param topic 主题:医院id
     * @param key 值:就诊流水号
     * @param value 值:审核结果,通过success,未通过error
     */
    public static void sendDataToKafka(String topic,String key,String value){
        if (producer == null){
            initKafkaProducer();
        }
        ProducerRecord<String,String> message = new ProducerRecord<String, String>(topic,key,value);
    }

    /**
     * 初始化producer对象
     */
    private static void initKafkaProducer(){
        Properties pro = new Properties();
        pro.put("bootstrap.servers", ConfigFactory.kafkaip+":"+ConfigFactory.kafkaport);
        pro.put("acks", "0");
        pro.put("retries", 0);
        pro.put("batch.size", 16384);
        pro.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pro.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(pro);
    }
}


