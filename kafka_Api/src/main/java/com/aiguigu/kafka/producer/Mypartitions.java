package com.aiguigu.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author lyc
 * @version V1.0.0
 * @date 2022/5/20 14:19
 */
public class Mypartitions implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        String msgvalue = o1.toString();
        int Partition;
        if(msgvalue.contains("atiguigu")){
            Partition = 0;
        }else {
            Partition =1;
        }
        return Partition;
    }

    @Override
    public void close() {
    }

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        Partitioner.super.onNewBatch(topic, cluster, prevPartition);
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}




