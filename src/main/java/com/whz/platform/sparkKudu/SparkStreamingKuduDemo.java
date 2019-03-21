package com.whz.platform.sparkKudu;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.alibaba.fastjson.JSON;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * @author peiyu
 */
public class SparkStreamingKuduDemo {

    private static final String EVENT_CODE = "test";

    private static final String KUDU_TABLE_NAME = "";

    public static void main(String[] args) throws Exception {
    	String topic = "";
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("keap-stream-app-" + EVENT_CODE);
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");//确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失

        JavaStreamingContext context = new JavaStreamingContext(sparkConf, Seconds.apply(6));


        KuduContext kuduContext = new KuduContext("172.172.241.228:7051", context.ssc().sc());

        KuduTable kuduTable = kuduContext.syncClient().openTable(KUDU_TABLE_NAME);
        List<ColumnSchema> columns = kuduTable.getSchema().getColumns();


        // 首先要创建一份kafka参数map
        Map<String, String> kafkaParams = new HashMap<>();
        // 这里是不需要zookeeper节点,所以这里放broker.list
        kafkaParams.put("metadata.broker.list",
                "master:9092,slave1:9092,slave2:9092");
        kafkaParams.put("auto.offset.reset", "smallest");
        kafkaParams.put("group.id", "keap-stream-" + EVENT_CODE);

        Set<String> topicSet = new HashSet<String>();
        topicSet.add(topic);
        
        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(context, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicSet);

        //处理数据
        JavaPairDStream<List<StructField>, Object[]> pairDStream = stream.mapToPair(tuple2 -> {
            Map<String, String> dataMap = parseData(tuple2._2);
            List<StructField> structFields = new ArrayList<>();
            dataMap.keySet().forEach(k -> {
                StructField field = getField(columns.stream().filter(it -> it.getName().equals(k)).findFirst().orElseThrow(() -> new NullPointerException("kudu table don't exist:" + k)));
                structFields.add(field);
            });
            Object[] objects = dataMap.values().toArray();
            return new Tuple2<>(structFields, objects);
        });

        context.start();
        context.awaitTermination();
        context.close();
    }

    private static Map<String, String> parseData(String json) {
        Map<String, String> map = new HashMap<>();
        JSON.parseObject(json).getJSONObject("req").getJSONObject("content").getInnerMap().forEach((k, v) -> map.put(k, v.toString()));
        return map;
    }

    private static StructField getField(ColumnSchema column) {
        return DataTypes.createStructField(column.getName(), convertType(column.getType()), true);
    }

    private static DataType convertType(Type type) {
        switch (type) {
            case UNIXTIME_MICROS:
                return DataTypes.TimestampType;
            case BINARY:
                return DataTypes.BinaryType;
            case STRING:
                return DataTypes.StringType;
            case DOUBLE:
                return DataTypes.DoubleType;
            case FLOAT:
                return DataTypes.FloatType;
            case INT64:
                return DataTypes.LongType;
            case INT32:
                return DataTypes.IntegerType;
            case INT16:
                return DataTypes.ShortType;
            case INT8:
                return DataTypes.ByteType;
            case BOOL:
                return DataTypes.BooleanType;
            default:
                return DataTypes.StringType;
        }
    }
}
