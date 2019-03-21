package flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * https://juejin.im/post/5bf93517f265da611510760d
 */
public class KafkaOffsetConfig {




    public static void init(Properties properties) {

        //订单产生、下单、取消、出库、签收
        String servers="10.253.11.113:9092,10.253.11.131:9092,10.253.11.64:9092";
        properties.setProperty("zookeeper.connect","10.253.11.113,10.253.11.131,10.253.11.64:2181/kafka");
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", "ourui-flink");
        //properties.setProperty("enable.auto.commit","true");
        //properties.setProperty("auto.commit.enable","true");


    }

    public static void main(String[] args) throws Exception {


        Properties properties = new Properties();
        String topic = "90001-whstask046-oms-orders";
        init(properties);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<>(topic,
                new SimpleStringSchema(), properties);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.setStateBackend(new FsStateBackend("file:///tmp/flinktmp"));
        env.enableCheckpointing(1000);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        kafkaConsumer.setStartFromLatest();
        DataStream<String> stream = env.addSource(kafkaConsumer);


        DataStream<JSONObject> flatMapstream= stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out)
                    throws Exception {
                JSONArray datas = JSONArray.parseArray(value);
                for(int i = 0;i < datas.size();i++){
                    String str = datas.getString(i).replace("\n\t", "");
                    JSONObject content = JSON.parseObject(str);
                    out.collect(content);

                }

            }
        });

        flatMapstream.print();
        env.execute();

    }
}
