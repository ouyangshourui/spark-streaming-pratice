package com.whz.streaming.flink;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.hbase.HBaseUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * mvn clean package -Pprod
 */
public class KafkaDealTradeTableTest {

    private static final long serialVersionUID = 1L;
    private static String aTable;
    private static String bTable;
    private static final String defaultTimeString = "1970-01-01 00:00:00";
    private static final String schema_size = "schema_size";
    private static final String kuduTable = "event.real_time_event_tenancy_user_sku_order_kudu_de";
    private static final String kudumaster = "10.253.240.11,10.253.240.16,10.253.240.14";


    private static final String[] bTableRemoveSchemaArray = {"optTime", "optType", "selfTimeSequence",
            "tableName", "databaseName", "Fdeal_id",
            "Fbdeal_id", "Ftenancy_id", "Fbuyer_id"};


    public static final String[] columnFamilys = {"info"};


    public static void init(Properties properties) {
        //订单产生、下单、取消、出库、签收
        String servers = "10.253.11.113:9092,10.253.11.131:9092,10.253.11.64:9092";
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", "KafkaDealTradeTable2EventRealTime");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.enable", "true");
        //properties.setProperty("auto.offset.reset", "earliest");

    }


    /**
     * @param args flink run -m yarn-cluster -yn 4 -yjm 2048 -ytm 2048 -ys 1 -ynm KafkaDealTradeTable2EventRealTime
     *             -c KafkaDealTradeTable2EventRealTime k-eap-streaming.jar
     *             initABTableFlag StartFromTimestamp
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("flink run -m yarn-cluster -yn 4 -yjm 2048 -ytm 2048 -ys 1 -ynm KafkaDealTradeTable2EventRealTime \n" +
                    "        -c KafkaDealTradeTable2EventRealTime k-eap-streaming.jar\n" +
                    "        initABTableFlag StartTodayZeroPointTimestampsFlag Parallelism");
        }

        Properties properties = new Properties();
        String topic = "90001-whstask046-oms-orders";
        init(properties);
        HBaseUtil.setZookeeper("10.254.1.153,10.254.0.81");

        KafkaDealTradeTableTest tradeTable2EventRealTime = new KafkaDealTradeTableTest();
        tradeTable2EventRealTime.setaTable("t_deal");
        tradeTable2EventRealTime.setbTable("t_trade");
        String aTable = tradeTable2EventRealTime.getaTable();
        String bTable = tradeTable2EventRealTime.getbTable();


        HBaseUtil hbaseInstance = HBaseUtil.getInstance();


        //需要设置参数，选择初始化


        if (Boolean.parseBoolean(args[0]) == true) {
            System.out.println("start init hbase table ");
            if (hbaseInstance.existTable(tradeTable2EventRealTime.getaTable()))
                hbaseInstance.dropTable(tradeTable2EventRealTime.getaTable());
            if (hbaseInstance.existTable(tradeTable2EventRealTime.getbTable()))
                hbaseInstance.dropTable(tradeTable2EventRealTime.getbTable());
            hbaseInstance.createTable(tradeTable2EventRealTime.getaTable(), columnFamilys);
            hbaseInstance.createTable(tradeTable2EventRealTime.getbTable(), columnFamilys);
            System.out.println("finish init hbase table ");
        }

//        String kudumasters="10.253.240.11,10.253.240.16,10.253.240.14";
//        Set<String> notNullColumns = new HashSet<String>();//不为空的列
//        List<Map<String,Object>> columnList = initKudu(kudumasters, kuduTable,notNullColumns);

        //获取t_deal,t_trade 流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        int parallesim = Integer.parseInt(args[2]);
        //env.setStateBackend(new FsStateBackend("hdfs:///user/flink/checkpoints"));
        env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints"));
        env.setParallelism(parallesim);

        //从昨天的23：30开始
        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<>(topic,
                new SimpleStringSchema(), properties);

        DataStream<String> stream = env.addSource(kafkaConsumer, "kafka");

//        DataStream<String> tupleFilterstream = stream.map(new MapFunction<String, String>() {
//
//            @Override
//            public String map(String s) throws Exception {
//                System.out.println("data is"+s);
//                return s;
//            }
//        }).setParallelism(parallesim);
//
//
//        tupleFilterstream.print();


        //topic "\n\t" 字段需要去掉
        DataStream<JSONObject> flatMapstream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out)
                    throws Exception {
                JSONArray datas = JSONArray.parseArray(value);
                for (int i = 0; i < datas.size(); i++) {
                    String str = datas.getString(i).replace("\n\t", "").toLowerCase();
                    JSONObject content = JSON.parseObject(str);
                    out.collect(content);

                }

            }
        }).setParallelism(parallesim);


        flatMapstream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<JSONObject>() {
                    @Override
                    public long extractAscendingTimestamp(JSONObject element) {
                        return Long.parseLong(element.getString("opttime"));
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<JSONObject, Object, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<JSONObject> elements, Collector<Object> out) throws Exception {

                        Iterator<JSONObject> iter = elements.iterator();
                        while (iter.hasNext()) {
                            System.out.println(iter.next());
                        }

                    }
                });

//
//        /**
//         * 过滤掉不是a,b表的数据 ,不是insert或者update的数据
//         */
//
//        DataStream<JSONObject> filterstream= flatMapstream.filter(new FilterFunction<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                Boolean optTypeFlag=false;
//                Boolean tableNameFlag=false;
//                String tableName= value.getString("tablename");
//                String optType= value.getString("opttype");
//                if(optType.equals("insert")||optType.equals("update")){
//                    optTypeFlag=true;
//                }
//                if(tableName.equals(aTable)||tableName.equals(bTable)){
//                    tableNameFlag=true;
//                }
//                return tableNameFlag && optTypeFlag;
//            }
//        }).setParallelism(1);
//
//        DataStream<Tuple2<Integer, JSONObject>> tupleFilterstream = filterstream.map(new MapFunction<JSONObject, Tuple2<Integer, JSONObject>>() {
//            @Override
//            public Tuple2<Integer, JSONObject> map(JSONObject jsonObject) throws Exception {
//                String ftenancy_id = jsonObject.getString("ftenancy_id");
//                String fbuyer_id = jsonObject.getString("fbuyer_id");
//                return new Tuple2<>((ftenancy_id + fbuyer_id).hashCode(), jsonObject);
//            }
//        }).setParallelism(1);
//
//
//        KeyedStream<Tuple2<Integer, JSONObject>, Integer> keybytupleFilterstream = tupleFilterstream.keyBy(new KeySelector<Tuple2<Integer, JSONObject>, Integer>() {
//            @Override
//            public Integer getKey(Tuple2<Integer, JSONObject> integerJSONObjectTuple2) throws Exception {
//                return Math.abs(integerJSONObjectTuple2.f0) % parallesim;
//
//            }
//        });
//
//
//
//        DataStream<JSONObject> resultstream = keybytupleFilterstream.map(new MapFunction<Tuple2<Integer, JSONObject>, JSONObject>() {
//            @Override
//            public JSONObject map(Tuple2<Integer, JSONObject> integerJSONObjectTuple2) throws Exception {
//                return integerJSONObjectTuple2.f1;
//            }
//        }).setParallelism(parallesim);

        // resultstream.print();

        env.execute();

    }

    public static void initEventMap(Map<String, String> event_type_map, Map<String, String> event_time_map) {
        event_type_map.put("fdeal_gen", "1");
        event_type_map.put("fdeal_pay_suss", "2");
        event_type_map.put("fdeal_cancel", "3");
        event_type_map.put("fdeal_consign", "4");
        event_type_map.put("fdeal_recv_confirm", "5");
        event_time_map.put("fdeal_gen", "fdeal_gen_time");
        event_time_map.put("fdeal_pay_suss", "fdeal_pay_suss_time");
        event_time_map.put("fdeal_cancel", "fdeal_cancel_time");
        event_time_map.put("fdeal_consign", "fdeal_consign_time");
        event_time_map.put("fdeal_recv_confirm", "fdeal_recv_confirm_time");
    }


    public static void initHbaseUtil() {
        HBaseUtil.setZookeeper("10.254.1.153,10.254.0.81");
    }


    /**
     * @param:加上8小时的毫秒数是因为毫秒时间戳是从北京时间1970年01月01日08时00分00秒开始算的 这样今天零点取oneDayTimestamps的余就是0
     * @Description: 获得“今天”零点时间戳 获得2点的加上2个小时的毫秒数就行
     */
    public static Long getTodayZeroPointTimestamps() {
        Long currentTimestamps = System.currentTimeMillis();
        Long oneDayTimestamps = Long.valueOf(60 * 60 * 24 * 1000);
        return currentTimestamps - (currentTimestamps + 60 * 60 * 8 * 1000) % oneDayTimestamps;
    }

    /**
     * @param fevent_time
     * @param jsonData
     */
    public static void getCollectData(
            TreeMap<String, String> fevent_time, JSONObject jsonData,
            Map<String, String> aTable_map, Map<String, String> bTable_map,
            Collector<Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, Object>>> out, Map<String, String> event_type_map, Map<String, String> event_time_map) {

        Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, Object>> data = new Tuple2();

        for (String event_time : event_time_map.keySet()) {
            if (aTable_map.get(event_time_map.get(event_time)).compareTo(defaultTimeString) > 0)
                fevent_time.put(event_time, aTable_map.get(event_time_map.get(event_time)));
        }

        //获取下单、支付、出库、出库、签收最大的事件，并找到相应事件，然后写入相关事件
        Map.Entry<String, String> max_time = fevent_time.entrySet().stream()
                .max((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
                .get();
        for (String event_time : fevent_time.keySet()) {
            if (fevent_time.get(event_time).equals(max_time.getValue())) {
                Tuple9<String, String, String, String, String, String, String, String, String> abTablekey = new Tuple9();
                abTablekey.setField(jsonData.getString("ftenancy_id"), 0);
                abTablekey.setField(fevent_time.get(event_time).substring(0, 10), 1);
                abTablekey.setField(jsonData.getString("fbuyer_id"), 2);
                abTablekey.setField(fevent_time.get(event_time), 3);
                abTablekey.setField(bTable_map.get("ftrade_id"), 4);
                abTablekey.setField(aTable_map.get("fdeal_id"), 5);
                abTablekey.setField(bTable_map.get("fitem_sku_id"), 6);
                abTablekey.setField(aTable_map.get("fbdeal_id"), 7);
                abTablekey.setField(event_type_map.get(event_time), 8);
                data.setField(abTablekey, 0);
                aTable_map.putAll(bTable_map);
                data.setField(aTable_map, 1);
                out.collect(data);
            }

        }
    }


    public String getaTable() {
        return aTable;
    }

    public void setaTable(String aTable) {
        this.aTable = aTable;
    }

    public String getbTable() {
        return bTable;
    }

    public void setbTable(String bTable) {
        this.bTable = bTable;
    }


}


