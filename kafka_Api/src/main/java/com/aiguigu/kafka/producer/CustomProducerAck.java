package com.aiguigu.kafka.producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author lyc
 * @version V1.0.0
 * @date 2022/5/19 16:42
 */
public class CustomProducerAck {
    public static void main(String[] args) {
        //配置
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        //key value的序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        //acks
        properties.put(ProducerConfig.ACKS_CONFIG,"1");

        //重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");



        //1.创建kafka生成者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        //2.发送数据
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first","hello1"));
        }
        //3.关闭资源
        kafkaProducer.close();
    }
}
