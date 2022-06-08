package com.aiguigu.kafka.consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author lyc
 * @version V1.0.0
 * @date 2022/5/28 9:11
 *
 */
public class Consumer03 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test01");
        //创建生产者对象
        KafkaConsumer kafkaConsumer = new KafkaConsumer<>(properties);


        //创建topics列表
        ArrayList<String> Topics = new ArrayList<>();
        Topics.add("third");
        kafkaConsumer.subscribe(Topics);


        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : records) {
                System.out.println(consumerRecord);
            }
        }
    }
}
