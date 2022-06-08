package com.aiguigu.kafka.Test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author lyc
 * @version V1.0.0
 * @date 2022/5/27 16:32
 */
public class Producer01 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //1.创建kafka生产者对象
        KafkaProducer kafkaProducer = new KafkaProducer<String, String>(properties);
        //2.发送数据
        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord("third", "sorry" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        System.out.println("主题:"+recordMetadata.topic()+"分区:"+recordMetadata.partition());
                    }
                }
            }).get();
        }
        //key value的序列化
        kafkaProducer.close();
        //3.关闭资源

    }
}
