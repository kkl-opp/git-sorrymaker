package com.aiguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author lyc
 * @version V1.0.0
 * @date 2022/5/19 16:42
 */
public class CustomTransactions {
    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Properties properties = new Properties();
        //key value的序列化
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"tans_01");





        //1.创建kafka生产者对象
        KafkaProducer kafkaProducer = new KafkaProducer<String, String>(properties);


        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();
        try {
            for (int i = 0; i < 3; i++) {
                kafkaProducer.send(new ProducerRecord("second","sws" + i));

            }
            kafkaProducer.commitTransaction();

        }catch (Exception e){
            kafkaProducer.abortTransaction();
        }finally {
            kafkaProducer.close();
        }


        //3.关闭资源

    }
}

