package com.whz.streaming.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;


/**
 * @Description hbase reader
 * @Author ourui
 * @Date 2019/02/22
 **/
public class KafkaSingleSource extends RichSourceFunction<ConsumerRecords<String, String>> implements Serializable {
    private static final long serialVersionUID = 3L;
    private static final Logger logger = LoggerFactory.getLogger(KafkaSingleSource.class);

    private KafkaConsumer<String, String>  kafkaConsumer = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.253.11.113:9092,10.253.11.131:9092,10.253.11.64:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "KafkaSingleSource");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,1000);
        kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(Arrays.asList("90001-whstask046-oms-orders"));

    }

    @Override
    public void run(SourceContext<ConsumerRecords<String, String> > ctx) throws Exception {
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            ctx.collect(records);
        }

    }

    @Override
    public void cancel() {


    }
}


