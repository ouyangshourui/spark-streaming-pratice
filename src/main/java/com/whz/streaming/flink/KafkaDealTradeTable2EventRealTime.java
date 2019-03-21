package com.whz.streaming.flink;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.hbase.HBaseUtil;
import com.whz.platform.sparkKudu.model.HbaseCell;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.*;

/**
 * mvn clean package -Pprod
 */
public class KafkaDealTradeTable2EventRealTime {

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

    private final static AscendingTimestampExtractor extractor = new AscendingTimestampExtractor<JSONObject>() {
        @Override
        public long extractAscendingTimestamp(JSONObject element) {
            return Long.parseLong(element.getString("opttime"));
        }
    };


    public static void init(Properties properties) {
        //订单产生、下单、取消、出库、签收
        String servers = "10.253.11.113:9092,10.253.11.131:9092,10.253.11.64:9092";
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", "KafkaDealTradeTable2EventRealTime");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.enable", "true");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
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

        KafkaDealTradeTable2EventRealTime tradeTable2EventRealTime = new KafkaDealTradeTable2EventRealTime();
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


        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<>(topic,
                new SimpleStringSchema(), properties);


        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        if (Boolean.parseBoolean(args[1]) == true) {
            System.out.println("kafkaConsumer Start From yesterday 23:30");
            kafkaConsumer.setStartFromTimestamp(getTodayZeroPointTimestamps() - 1000 * 60 * 30);

        } else {
            System.out.println("kafkaConsumer Start From Group Offsets:" + properties.getProperty("group.id"));
            kafkaConsumer.setStartFromGroupOffsets();
        }


        int parallesim = Integer.parseInt(args[2]);
         //env.setStateBackend(new FsStateBackend("hdfs:///user/flink/checkpoints"));
        env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints"));
        env.setParallelism(parallesim);

        //从昨天的23：30开始


        DataStream<String> stream = env.addSource(kafkaConsumer, "kafka").setParallelism(parallesim);

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

        /**
         * 过滤掉不是a,b表的数据 ,不是insert或者update的数据
         */

        DataStream<JSONObject> filterstream = flatMapstream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                Boolean optTypeFlag = false;
                Boolean tableNameFlag = false;
                String tableName = value.getString("tablename");
                String optType = value.getString("opttype");
                if (optType.equals("insert") || optType.equals("update")) {
                    optTypeFlag = true;
                }
                if (tableName.equals(aTable) || tableName.equals(bTable)) {
                    tableNameFlag = true;
                }
                return tableNameFlag && optTypeFlag;
            }
        }).setParallelism(parallesim);

        //       DataStream<Tuple2<Integer, JSONObject>> tupleFilterstream = filterstream.map(new MapFunction<JSONObject, Tuple2<Integer, JSONObject>>() {
//            @Override
//            public Tuple2<Integer, JSONObject> map(JSONObject jsonObject) throws Exception {
//                String ftenancy_id = jsonObject.getString("ftenancy_id");
//                String fbuyer_id = jsonObject.getString("fbuyer_id");
//                return new Tuple2<>((ftenancy_id + fbuyer_id).hashCode(), jsonObject);
//            }
//        }).setParallelism(parallesim);
//
//
//        KeyedStream<Tuple2<Integer, JSONObject>, Integer> keybytupleFilterstream = tupleFilterstream.keyBy(new KeySelector<Tuple2<Integer, JSONObject>, Integer>() {
//            @Override
//            public Integer getKey(Tuple2<Integer, JSONObject> integerJSONObjectTuple2) throws Exception {
//                return Math.abs(integerJSONObjectTuple2.f0) % (2*parallesim);
//
//            }
//        });
//
//
//        DataStream<JSONObject> resultstream = keybytupleFilterstream.map(new MapFunction<Tuple2<Integer, JSONObject>, JSONObject>() {
//            @Override
//            public JSONObject map(Tuple2<Integer, JSONObject> integerJSONObjectTuple2) throws Exception {
//                return integerJSONObjectTuple2.f1;
//            }
//        }).setParallelism(2*parallesim);


        //将A，B变写入到HBase，方便相互查询

        DataStream<JSONObject> Load2HBasestream = filterstream.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonData) throws Exception {
                //long start_currentTime = System.currentTimeMillis();
                initHbaseUtil();
                HBaseUtil hbaseUtil = HBaseUtil.getInstance();
                String tablename = jsonData.getString("tablename");
                if (aTable.equals(tablename)) {
                    String a_table_key = jsonData.getString("fdeal_id")
                            + '_' + jsonData.getString("fbdeal_id")
                            + '_' + jsonData.getString("ftenancy_id")
                            + '_' + jsonData.getString("fbuyer_id");

                    hbaseUtil.insertData(aTable, a_table_key, columnFamilys[0],
                            jsonData);
                    //System.out.println("write a table to hbase: "+tablename+":"+a_table_key);
                }

                if (bTable.equals(tablename)) {
                    String b_table_key = jsonData.getString("fdeal_id")
                            + '_' + jsonData.getString("fbdeal_id")
                            + '_' + jsonData.getString("ftenancy_id")
                            + '_' + jsonData.getString("fbuyer_id")
                            + '_' + jsonData.getString("ftrade_id");
                    hbaseUtil.insertData(bTable, b_table_key, columnFamilys[0],
                            jsonData);
                    //  System.out.println("write b table to hbase: "+tablename+":"+b_table_key);
                }
                long end_currentTime = System.currentTimeMillis();
                //System.out.println("Load2HBasestream time:"+(end_currentTime-start_currentTime));
                return jsonData;
            }
        }).setParallelism(parallesim);


        DataStream<Tuple2<Integer, JSONObject>> tupleFilterstream = Load2HBasestream.map(new MapFunction<JSONObject, Tuple2<Integer, JSONObject>>() {
            @Override
            public Tuple2<Integer, JSONObject> map(JSONObject jsonObject) throws Exception {
                String ftenancy_id = jsonObject.getString("ftenancy_id");
                String fbuyer_id = jsonObject.getString("fbuyer_id");
                Integer code = (ftenancy_id + fbuyer_id).hashCode() % 10;
                return new Tuple2<>(code > 0 ? code : -code, jsonObject);
            }
        }).setParallelism(parallesim);


        KeyedStream<Tuple2<Integer, JSONObject>, Integer> keybytupleFilterstream = tupleFilterstream.keyBy(new KeySelector<Tuple2<Integer, JSONObject>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, JSONObject> integerJSONObjectTuple2) throws Exception {
                return Math.abs(integerJSONObjectTuple2.f0);

            }
        });


        SingleOutputStreamOperator<ArrayList<Tuple3<String, String, Map<String, String>>>> windowStream = Load2HBasestream.assignTimestampsAndWatermarks(extractor)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new ProcessAllWindowFunction<JSONObject, ArrayList<Tuple3<String, String, Map<String, String>>>, Window>() {
                    @Override
                    public void process(Context context, Iterable<JSONObject> elements, Collector<ArrayList<Tuple3<String, String, Map<String, String>>>> out) throws Exception {
                        ArrayList<Tuple3<String, String, Map<String, String>>> sortMapdata = new ArrayList<>();
                        //tablename,key,valule
                        Iterator<JSONObject> iter = elements.iterator();

                        while (iter.hasNext()) {
                            Tuple3<String, String, Map<String, String>> table = new Tuple3<>();
                            Map<String, String> tableMap = new HashMap();
                            JSONObject jsonData = iter.next();
                            String tablename = jsonData.getString("tablename");
                            table.f0 = tablename;
                            if (aTable.equals(tablename)) {
                                String a_table_key = jsonData.getString("fdeal_id")
                                        + '_' + jsonData.getString("fbdeal_id")
                                        + '_' + jsonData.getString("ftenancy_id")
                                        + '_' + jsonData.getString("fbuyer_id");
                                table.f1 = a_table_key;

                            } else {
                                String b_table_key = jsonData.getString("fdeal_id")
                                        + '_' + jsonData.getString("fbdeal_id")
                                        + '_' + jsonData.getString("ftenancy_id")
                                        + '_' + jsonData.getString("fbuyer_id");
                                table.f1 = b_table_key;
                            }

                            for (String key : jsonData.keySet()) {
                                tableMap.put(key, jsonData.getString(key));
                            }
                            table.f2 = tableMap;
                            sortMapdata.add(table);


                        }
                        //System.out.println("window 30s:" + sortMapdata.toString());
                        out.collect(sortMapdata);

                    }
                }).setParallelism(1);

        DataStream<ArrayList<Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, String>>>> batchStream = windowStream.map(new MapFunction<ArrayList<Tuple3<String, String, Map<String, String>>>, ArrayList<Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, String>>>>() {
            @Override
            public ArrayList<Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, String>>> map(ArrayList<Tuple3<String, String, Map<String, String>>> tuple3s) throws Exception {

                ArrayList<Tuple3<String, String, Map<String, String>>> aTablesortMapdata = new ArrayList<>();
                ArrayList<String> aTable_key = new ArrayList<>();
                ArrayList<String> bTable_key = new ArrayList<>();
                initHbaseUtil();
                ArrayList<Tuple3<String, String, Map<String, String>>> bTablesortMapdata = new ArrayList<>();

                for (Tuple3<String, String, Map<String, String>> tuple3 : tuple3s) {
                    String tablename = tuple3.f0;
                    if (aTable.equals(tablename)) {
                        aTablesortMapdata.add(tuple3);
                        aTable_key.add(tuple3.f1);

                    } else {
                        bTablesortMapdata.add(tuple3);
                        bTable_key.add(tuple3.f1);
                    }
                }


                long start_currentTime = System.currentTimeMillis();
                // 这里读取有问题
                Map<String, List<HbaseCell>> aHBaseMapData = HBaseUtil.getInstance().prefixFilters(bTable, "info", aTable_key);
                long end_currentTime = System.currentTimeMillis();
                System.out.println("aHBaseMapData hbase time:" + (end_currentTime - start_currentTime));

                long start1_currentTime = System.currentTimeMillis();
                Map<String, List<HbaseCell>> bHBaseMapData = HBaseUtil.getInstance().getRow(aTable, "info", bTable_key);
                long end_currentTime1 = System.currentTimeMillis();
                System.out.println("bHBaseMapData hbase time:" + (end_currentTime1 - start1_currentTime));

                ArrayList<Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, String>>> data = new ArrayList();
                for (Tuple3<String, String, Map<String, String>> tuple : aTablesortMapdata) {
                    Map<String, String> aJoinbTable_map = new HashMap();
                    //需要模糊匹配
                    for(String inkey:aHBaseMapData.keySet()){
                        if(inkey.startsWith(tuple.f1)){
                            List<HbaseCell> list = aHBaseMapData.get(inkey);
                            if (list != null) {
                                for (HbaseCell hbasecell : list) {
                                    aJoinbTable_map.put(hbasecell.getColumn(), (String) hbasecell.getValue());
                                }
                                getCollectData(tuple.f2, aJoinbTable_map, data);
                            }

                        }
                    }
                }


                for (Tuple3<String, String, Map<String, String>> tuple : bTablesortMapdata) {
                    Map<String, String> bJoinaTable_map = new HashMap();
                    List<HbaseCell> list = bHBaseMapData.get(tuple.f1);
                    if (list != null) {
                        for (HbaseCell hbasecell : list) {
                            bJoinaTable_map.put(hbasecell.getColumn(), (String) hbasecell.getValue());
                        }
                        getCollectData(bJoinaTable_map, tuple.f2, data);
                    }
                }

                return data;
            }
        }).setParallelism(1);



//        //A，B表关联，返回一个Tuple2，方便写入到kafka或者写入到kudu。key设计参考任务：商品化事实宽表-商品订单宽表-天写入到kudu的key设计
//        DataStream<Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, Object>>> ABTableFlatMapStream = Load2HBasestream.flatMap(
//                new FlatMapFunction<JSONObject, Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, Object>>>() {
//                    @Override
//                    public void flatMap(JSONObject jsonData, Collector<Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, Object>>> out)
//                            throws Exception {
//                        long start_currentTime = System.currentTimeMillis();
//                        Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, String>> data = new Tuple2();
//                        HBaseUtil hbaseUtil = HBaseUtil.getInstance();
//
//                        String tablename = jsonData.getString("tablename");
//
//                        Map<String, String> aTable_schema = new HashMap();
//                        Map<String, String> aTable_map = new HashMap();
//                        Map<String, String> bTable_schema = new HashMap();
//                        Map<String, String> bTable_map = new HashMap();
//
//                        TreeMap<String, String> fevent_time = new TreeMap<>();
//
//                        //传不进来
//                        Map<String, String> event_type_map = new HashMap<>();
//                        Map<String, String> event_time_map = new HashMap<>();
//                        initEventMap(event_type_map, event_time_map);
//                        initHbaseUtil();
//
//                        /**
//                         * A,B两张表都有的字段：info:databaseName
//                         *info:optTime
//                         *info:optType
//                         *info:selfTimeSequence
//                         *info:tableName
//                         */
//                        if (aTable.equals(tablename)) {
//                            String b_table_key_prefix = jsonData.getString("fdeal_id")
//                                    + '_' + jsonData.getString("fbdeal_id")
//                                    + '_' + jsonData.getString("ftenancy_id")
//                                    + '_' + jsonData.getString("fbuyer_id");
//
//                            for (String key : jsonData.keySet()) {
//                                aTable_schema.put(key, aTable);
//                            }
//
//                            aTable_schema.remove("tablename");
//                            aTable_schema.remove("databaseame");
//
//                            for (String key : jsonData.keySet()) {
//                                if (aTable_schema.containsKey(key))
//                                    aTable_map.put(key, jsonData.getString(key));
//                            }
//
//                            List<List<HbaseCell>> listrow = hbaseUtil.prefixFilter(bTable, columnFamilys[0], b_table_key_prefix);
//                            if (listrow.size() > 0) {
//
//                                for (HbaseCell hbasecell : listrow.get(0)) {
//                                    bTable_schema.put(hbasecell.getColumn(), bTable);
//                                }
//
//
////                                for(int i=0;i<bTableRemoveSchemaArray.length-1;i++){
////                                    bTable_schema.remove(bTableRemoveSchemaArray[i]);
////
////                                }
//
//                                //需要解决a表只读到b表部分字段,等待读完b字段
//                                for (List<HbaseCell> celllist : listrow) {
//                                    for (HbaseCell hbasecell : celllist) {
//                                        // if(bTable_schema.containsKey(hbasecell.getColumn())){
//                                        bTable_map.put(hbasecell.getColumn(), (String) hbasecell.getValue());
//                                        //   }
//                                    }
//                                    if (!bTable_map.isEmpty())
//                                        getCollectData(fevent_time, jsonData, aTable_map, bTable_map, out, event_type_map, event_time_map);
//                                    bTable_map.clear();
//                                    long end_currentTime = System.currentTimeMillis();
//                                    System.out.println("a.join b getCollectData time:" + (end_currentTime - start_currentTime));
//
//
//                                }
//
//
//                            }
//
//                        } else {
//                            String a_table_key = jsonData.getString("fdeal_id")
//                                    + '_' + jsonData.getString("fbdeal_id")
//                                    + '_' + jsonData.getString("ftenancy_id")
//                                    + '_' + jsonData.getString("fbuyer_id");
//
//                            //获取B表数据，去掉几个无用字段，放入到一个map中
//                            for (String key : jsonData.keySet()) {
//                                bTable_schema.put(key, key);
//                            }
//
//
//                            List<List<HbaseCell>> aTableData = hbaseUtil.getRow(aTable, columnFamilys[0], a_table_key);
//                            //如果关联不上，直接丢弃
//                            if (aTableData.size() > 0) {
//                                //需要读到atable的数据，可能只读到部分字段
//                                for (List<HbaseCell> celllist : aTableData) {
//                                    for (HbaseCell hbasecell : celllist) {
//                                        aTable_map.put(hbasecell.getColumn(), (String) hbasecell.getValue());
//                                    }
//                                }
//
////                                //去掉B表中无用字段
////                                for(int i=0;i<bTableRemoveSchemaArray.length-1;i++){
////                                    if(null!=bTable_schema.get(bTableRemoveSchemaArray[i]))
////                                       bTable_schema.remove(bTableRemoveSchemaArray[i]);
////                                }
//                                for (String key : jsonData.keySet()) {
//                                    if (bTable_schema.containsKey(key))
//                                        bTable_map.put(key, jsonData.getString(key));
//                                }
//
//                                if (!aTable_map.isEmpty()) {
//                                    getCollectData(fevent_time, jsonData, aTable_map, bTable_map, out, event_type_map, event_time_map);
//                                    long end_currentTime = System.currentTimeMillis();
//                                    System.out.println("b.join a getCollectData time:" + (end_currentTime - start_currentTime));
//                                }
//                            }
//
//                        }
//
//
//                    }
//                }).setParallelism(parallesim);
        SinkTokudu sinkTokudu = new SinkTokudu();
        batchStream.addSink(sinkTokudu).name("kudu sink").setParallelism(1);
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
     * @param aTable_map
     * @param bTable_map
     * @param out
     */
    public static void getCollectData(Map<String, String> aTable_map, Map<String, String> bTable_map,
                                     ArrayList<Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, String>>> out) {

        TreeMap<String, String> fevent_time = new TreeMap<>();
        Map<String, String> event_type_map = new HashMap<>();
        Map<String, String> event_time_map = new HashMap<>();
        initEventMap(event_type_map, event_time_map);

        for (String event_time : event_time_map.keySet()) {
            if (aTable_map.get(event_time_map.get(event_time)).compareTo(defaultTimeString) > 0)
                fevent_time.put(event_time, aTable_map.get(event_time_map.get(event_time)));
        }

        //获取下单、支付、出库、出库、签收最大的事件，并找到相应事件，然后写入相关事件
        Map.Entry<String, String> max_time = fevent_time.entrySet().stream()
                .max((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
                .get();
        for (String event_time : fevent_time.keySet()) {
            Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, String>> tuple = new Tuple2<>();
            if (fevent_time.get(event_time).equals(max_time.getValue())) {
                Tuple9<String, String, String, String, String, String, String, String, String> abTablekey = new Tuple9();
                abTablekey.setField(aTable_map.get("ftenancy_id"), 0);
                abTablekey.setField(fevent_time.get(event_time).substring(0, 10), 1);
                abTablekey.setField(aTable_map.get("fbuyer_id"), 2);
                abTablekey.setField(fevent_time.get(event_time), 3);
                abTablekey.setField(bTable_map.get("ftrade_id"), 4);
                abTablekey.setField(aTable_map.get("fdeal_id"), 5);
                abTablekey.setField(bTable_map.get("fitem_sku_id"), 6);
                abTablekey.setField(aTable_map.get("fbdeal_id"), 7);
                abTablekey.setField(event_type_map.get(event_time), 8);
                aTable_map.putAll(bTable_map);
                tuple.f0=abTablekey;
                tuple.f1=aTable_map;
             //   System.out.println("getCollectData:"+tuple);
                out.add(tuple);
            }

        }
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


