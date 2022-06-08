package com.aiguigu.kafka.producer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author lyc
 * @version V1.0.0
 * @date 2022/5/19 16:42
 */
public class CustomProducerCallback {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        //key value的序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com/aiguigu/kafka/producer/Mypartitions");


        //1.创建kafka生成者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        //2.发送数据
        kafkaProducer.send(new ProducerRecord<>("first","hello2"),new Callback(){
            @Override
            public void onCompletion(RecordMetadata Metadata, Exception e) {
                if (e ==null){
                    System.out.println("主题:"+Metadata.topic()+" 分区:  "+Metadata.partition());
                }
            }
        });

        //3.关闭资源
        kafkaProducer.close();
    }
}
