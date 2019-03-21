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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.*;

public class Kafka2OmsABTable {

    private static final long serialVersionUID = 1L;
    private   String aTable;
    private   String bTable;
    //事件类型定义
    private  static Map<String,String> event_type_map = new HashMap<>();
    private  static Map<String,String> event_time_map = new HashMap<>();

    private   String anorelation;
    public  static  int printNum;

    public  static  final  String[] columnFamilys={"info"};


    public static void init(Properties properties) {
        //订单产生、下单、取消、出库、签收
        event_type_map.put("fdeal_gen","fdeal_gen");
        event_type_map.put("fdeal_pay_suss","fdeal_pay");
        event_type_map.put("fdeal_cancel","fdeal_cancel");
        event_type_map.put("fdeal_consign","fdeal_consign");
        event_type_map.put("fdeal_recv_confirm","Fdeal_recv_confirm");

        event_time_map.put("fdeal_gen","Fdeal_gen_time");
        event_time_map.put("fdeal_pay_suss","Fdeal_pay_suss_time");
        event_time_map.put("fdeal_cancel","fdeal_cancel_time");
        event_time_map.put("fdeal_consign","fdeal_consign_time");
        event_time_map.put("fdeal_recv_confirm","Fdeal_recv_confirm_time");


        String servers="10.253.11.113:9092,10.253.11.131:9092,10.253.11.64:9092";
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", "flink-java-test");
        properties.setProperty("auto.offset.reset", "earliest");

    }

    public static void main(String[] args)  throws  Exception {

        Properties properties = new Properties();
        String topic = "90001-whstask046-oms-orders";
        init(properties);
        HBaseUtil.setZookeeper("10.254.1.153,10.254.0.81");
//        if(null!=args[0])
   //         printNum=Integer.valueOf(args[0]);

        Kafka2OmsABTable kuduDealTradeTable = new Kafka2OmsABTable();
        kuduDealTradeTable.setaTable("t_deal");
        kuduDealTradeTable.setbTable("t_trade");
        kuduDealTradeTable.setAnorelation("t_deal_norelation_t_trade");
        String aTable="t_deal";
        String bTable="t_trade";
        String anorelation="t_deal_norelation_t_trade";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<>(topic,
                new SimpleStringSchema(), properties);

        DataStream<String> stream = env.addSource(kafkaConsumer);

//        HBaseReader hbasereader= new HBaseReader();
//        hbasereader.setTableName(anorelation);
//        hbasereader.setColFamily(columnFamilys[0]);
//        hbasereader.setZookeeper("10.254.1.153,10.254.0.81");
//        DataStream<JSONObject> hbaseStream = env.addSource(hbasereader);
//        hbaseStream.print();


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

        /**
         * 过滤掉不是a,b表的数据
         */

        DataStream<JSONObject> filterstream= flatMapstream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                Boolean optTypeFlag=false;
                Boolean tableNameFlag=false;
                String tableName= value.getString("tableName");
                String optType= value.getString("optType");
                if(optType.equals("INSERT")||optType.equals("UPDATE")){
                    optTypeFlag=true;
                }
                if(tableName.equals(aTable)||tableName.equals(bTable)){
                    tableNameFlag=true;
                }
                return tableNameFlag && optTypeFlag;
            }
        });



        DataStream<JSONObject> Load2HBasestream=filterstream.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonData) throws Exception {
                HBaseUtil hbaseUtil = HBaseUtil.getInstance();
                String tablename =jsonData.getString("tableName");
                if(aTable.equals(tablename)){
                    String a_table_key=jsonData.getString("Fdeal_id")
                            +'_'+jsonData.getString("Fbdeal_id")
                            +'_'+jsonData.getString("Ftenancy_id")
                            +'_'+jsonData.getString("Fbuyer_id")
                            +'_'+jsonData.getString("optType");

                    for(String key:jsonData.keySet()){
                        hbaseUtil.insertData(aTable,a_table_key,columnFamilys[0],
                                key,jsonData.getString(key));
                    }
                    //System.out.println("write a table to hbase: "+tablename+":"+a_table_key);
                }

                if(bTable.equals(tablename)){
                    String b_table_key=jsonData.getString("Fdeal_id")
                            +'_'+jsonData.getString("Fbdeal_id")
                            +'_'+jsonData.getString("Ftenancy_id")
                            +'_'+jsonData.getString("Fbuyer_id")
                            +'_'+jsonData.getString("Ftrade_id")
                            +'_'+jsonData.getString("optType");
                    for(String key:jsonData.keySet()){
                       hbaseUtil.insertData(bTable,b_table_key,columnFamilys[0],
                                key,jsonData.getString(key));
                    }
                     //  System.out.println("write b table to hbase: "+tablename+":"+b_table_key);
                }
                return  jsonData;
            }
        });




        DataStream<Tuple2<Tuple9<String,String,String,String,String,String,String,String,String>,Map<String,String>>> ABTableFlatMapStream= Load2HBasestream.flatMap(
                new FlatMapFunction<JSONObject, Tuple2<Tuple9<String,String,String,String,String,String,String,String,String>,Map<String,String>>>()  {
            @Override
            public void flatMap(JSONObject jsonData, Collector<Tuple2<Tuple9<String,String,String,String,String,String,String,String,String>,Map<String,String>>> out)
                    throws Exception {

                Tuple2<Tuple9<String,String,String,String,String,String,String,String,String>,Map<String,String>> data= new Tuple2();

                HBaseUtil hbaseUtil = HBaseUtil.getInstance();
                String tablename =jsonData.getString("tableName");
                String optType =jsonData.getString("optType");


                /**  　　 事件分析平台数据源定义：商品化商品订单事实表 可以定义
                 //租户号,
                 // 日期，
                 // userid，
                 // 事件发生的具体时间，
                 // 商品单号ftrade_id，
                 // 包裹单号fdeal_id，
                 // sku编码fskuid，
                 // 交易单号fbdeal_id，
                 // 事件类型fopt_type
                 **/


                Map<String,String> aTable_schema= new HashMap();
                Map<String,String> aTable_map= new HashMap();

                Map<String,String> bTable_schema= new HashMap();

                Map<String,String> abTable_relation_map = new HashMap<String, String>();
                Map<String,String> fdeal_time = new TreeMap<>();



                /**
                 * A,B两张表都有的字段：info:databaseName
                 *info:optTime
                 *info:optType
                 *info:selfTimeSequence
                 *info:tableName
                 */
                if(aTable.equals(tablename)){
                    for(String event_time:event_time_map.keySet()){
                        fdeal_time.put(event_time,jsonData.getString(event_time_map.get(event_time)));
                    }

                    String b_table_key_prefix=jsonData.getString("Fdeal_id")
                            +'_'+jsonData.getString("Fbdeal_id")
                            +'_'+jsonData.getString("Ftenancy_id")
                            +'_'+jsonData.getString("Fbuyer_id");


                    String a_table_key=jsonData.getString("Fdeal_id")
                            +'_'+jsonData.getString("Fbdeal_id")
                            +'_'+jsonData.getString("Ftenancy_id")
                            +'_'+jsonData.getString("Fbuyer_id")
                            +'_'+jsonData.getString("optType");

                    for(String key:jsonData.keySet()){
                        aTable_schema.put(key,aTable);
                    }
                    aTable_schema.remove("optTime");
                    aTable_schema.remove("optType");
                    aTable_schema.remove("selfTimeSequence");
                    aTable_schema.remove("tableName");
                    aTable_schema.remove("databaseName");


                    for(String key:jsonData.keySet()){
                        if(aTable_schema.containsKey(key))
                            aTable_map.put(key,jsonData.getString(key));
                    }




                    List<List<HbaseCell>> listrow=  hbaseUtil.prefixFilter(bTable,columnFamilys[0],b_table_key_prefix);
                    //如果A表关联上B表,如果区分update和insert,;如果关联不上，直接写入HBase
                    if(listrow.size()>0) {

                        for(String key:jsonData.keySet()){
                            if(aTable_schema.containsKey(key))
                                aTable_map.put(key,jsonData.getString(key));
                        }




                        for (HbaseCell hbasecell : listrow.get(0)) {
                            bTable_schema.put(hbasecell.getColumn(),bTable);
                        }
                        bTable_schema.remove("optTime");
                        bTable_schema.remove("optType");
                        bTable_schema.remove("selfTimeSequence");
                        bTable_schema.remove("tableName");
                        bTable_schema.remove("databaseName");
                        bTable_schema.remove("Fdeal_id");
                        bTable_schema.remove("Fbdeal_id");
                        bTable_schema.remove("Ftenancy_id");
                        bTable_schema.remove("Fbuyer_id");

                        for (List<HbaseCell> celllist : listrow) {
                            Map<String,String> bTable_map= new HashMap();
                            for (HbaseCell hbasecell : celllist) {

                                if(bTable_schema.containsKey(hbasecell.getColumn())){
                                    bTable_map.put(hbasecell.getColumn(), (String)hbasecell.getValue());
                                }
                            }



                            if (optType.equals("INSERT")) {
                                /**
                                 * 如果为insert，下单、支付、出库、出库、签收时间相同的话，几个事件都写入
                                 */

                                for(String event_type:event_type_map.keySet()){
                                    String fdeal_gen_time=jsonData.getString(event_time_map.get("fdeal_gen"));
                                    if(fdeal_gen_time.equals(jsonData.getString(event_time_map.get(event_type)))){
                                        Tuple9<String,String,String,String,String,String,String,String,String> key =new Tuple9();
                                        key.setField(jsonData.getString("Ftenancy_id"),0);
                                        key.setField(fdeal_gen_time.substring(0,9),1);
                                        key.setField(jsonData.getString("Fbuyer_id"),2);
                                        key.setField(fdeal_gen_time,3);
                                        key.setField(bTable_map.get("Ftrade_id"),4);
                                        key.setField(jsonData.getString("Fdeal_id"),5);
                                        key.setField(bTable_map.get("Fitem_sku_id"),6);
                                        key.setField(jsonData.getString("Fbdeal_id"),7);
                                        key.setField(event_type_map.get(event_type),8);
                                        bTable_map.putAll(aTable_map);
                                        data.setField(key,0);
                                        data.setField(bTable_map,1);
                                        out.collect(data);
                                    }
                                }
                            }else {
                                //不是下单事件,支付、出库、出库、签收时间和下单事件比较,取最大更新相应事件
                                //取最大事件
                                Map.Entry<String, String> max_time=fdeal_time.entrySet().stream()
                                        .max((e1,e2)->e1.getValue().compareTo(e2.getValue())).get();
                                if(max_time.getValue().compareTo(jsonData.getString(event_time_map.get("fdeal_gen")))>0){
                                    String event_type=event_type_map.get(max_time.getKey());
                                    Tuple9<String,String,String,String,String,String,String,String,String> key =new Tuple9();
                                    key.setField(jsonData.getString("Ftenancy_id"),0);
                                    key.setField(max_time.getValue().substring(0,9),1);
                                    key.setField(jsonData.getString("Fbuyer_id"),2);
                                    key.setField(max_time.getValue(),3);
                                    key.setField(bTable_map.get("Ftrade_id"),4);
                                    key.setField(jsonData.getString("Fdeal_id"),5);
                                    key.setField(bTable_map.get("Fitem_sku_id"),6);
                                    key.setField(jsonData.getString("Fbdeal_id"),7);
                                    key.setField(event_type_map.get(event_type),8);
                                    bTable_map.putAll(aTable_map);
                                    data.setField(key,0);
                                    data.setField(bTable_map,1);
                                    out.collect(data);
                                }

                            }
                        }

                    }else{  //如果A没有关联上B表，直接写入到A!B表中
                        for(String column:jsonData.keySet()){
                                aTable_map.put(column,jsonData.getString(column));
                            hbaseUtil.insertData(anorelation,a_table_key,columnFamilys[0],column,jsonData.getString(column));
                        }
                    }

                }else {
                    /**
                     * B insert 直接丢弃；B如果是update，B先和HBase B比较opt_time时间比较早，数据丢；，
                     * 如果晚的话，更新HBase，B去关联A,写入kudu
                     */
                    if(optType.equals("UPDATE")){


                        String b_table_key=jsonData.getString("Fdeal_id")
                                +'_'+jsonData.getString("Fbdeal_id")
                                +'_'+jsonData.getString("Ftenancy_id")
                                +'_'+jsonData.getString("Fbuyer_id")
                                +'_'+jsonData.getString("Ftrade_id");



                        String optTime=jsonData.getString("optTime");

                        Map<String, String> bTable_map = new HashMap();

                        for(String event_time:event_time_map.keySet()){
                            fdeal_time.put(event_time,jsonData.getString(event_time_map.get(event_time)));
                        }

                        if (optTime.compareTo(bTable_map.get("optTime"))>0){
                            List<List<HbaseCell>> aTablelistrow=  hbaseUtil.getRow(aTable,columnFamilys[0],b_table_key);
                            for (List<HbaseCell> celllist : aTablelistrow) {
                                Tuple9<String,String,String,String,String,String,String,String,String> key =new Tuple9();
                                Map<String, String> aTablelook_map = new HashMap();
                                for (HbaseCell hbasecell : celllist) {
                                    aTablelook_map.put(hbasecell.getColumn(), (String)hbasecell.getValue());
                                }

                                for(String event_time:event_time_map.keySet()){
                                    fdeal_time.put(event_time,aTablelook_map.get(event_time_map.get(event_time)));
                                }

                                if("INSERT".equals(aTablelook_map.get("optType"))){
                                    /**
                                     * 如果为insert，下单、支付、出库、出库、签收时间相同的话，几个事件都写入
                                     */

                                    for(String event_type:event_type_map.keySet()){
                                        String fdeal_gen_time=jsonData.getString(event_time_map.get("fdeal_gen"));
                                        if(fdeal_gen_time.equals(jsonData.getString(event_time_map.get(event_type)))){
                                            key.setField(aTablelook_map.get("Ftenancy_id"),0);
                                            key.setField(fdeal_gen_time.substring(0,9),1);
                                            key.setField(aTablelook_map.get("Fbuyer_id"),2);
                                            key.setField(fdeal_gen_time,3);
                                            key.setField(bTable_map.get("Ftrade_id"),4);
                                            key.setField(aTablelook_map.get("Fdeal_id"),5);
                                            key.setField(bTable_map.get("Fitem_sku_id"),6);
                                            key.setField(aTablelook_map.get("Fbdeal_id"),7);
                                            key.setField(event_type_map.get(event_type),8);
                                            data.setField(key,0);
                                            data.setField(bTable_map,1);
                                            out.collect(data);
                                        }
                                    }

                                }else {
                                    //不是下单事件,支付、出库、出库、签收时间和下单事件比较,取最大更新相应事件
                                    //取最大事件
                                    Map.Entry<String, String> max_time=fdeal_time.entrySet().stream()
                                            .max((e1,e2)->e1.getValue().compareTo(e2.getValue())).get();
                                    if(max_time.getValue().compareTo(aTablelook_map.get(event_time_map.get("fdeal_gen")))>0){
                                        String event_type=event_type_map.get(max_time.getKey());
                                        key.setField(jsonData.getString("Ftenancy_id"),0);
                                        key.setField(max_time.getValue().substring(0,9),1);
                                        key.setField(aTablelook_map.get("Fbuyer_id"),2);
                                        key.setField(max_time.getValue(),3);
                                        key.setField(aTablelook_map.get("Ftrade_id"),4);
                                        key.setField(jsonData.getString("Fdeal_id"),5);
                                        key.setField(aTablelook_map.get("Fitem_sku_id"),6);
                                        key.setField(jsonData.getString("Fbdeal_id"),7);
                                        key.setField(event_type_map.get(event_type),8);
                                        bTable_map.putAll(aTablelook_map);
                                        data.setField(key,0);
                                        data.setField(bTable_map,1);
                                        out.collect(data);
                                    }

                                }

                            }
                        }



                    }

                }


            }
        });



        DataStream<JSONObject> ABTableStream=Load2HBasestream.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonData) throws Exception {

                HBaseUtil hbaseUtil = HBaseUtil.getInstance();
                String tablename =jsonData.getString("tableName");
                String optType =jsonData.getString("optType");


                /**  　　 事件分析平台数据源定义：商品化商品订单事实表 可以定义
                 //租户号,
                // 日期，
                // userid，
                // 事件发生的具体时间，
                // 商品单号ftrade_id，
                // 包裹单号fdeal_id，
                // sku编码fskuid，
                // 交易单号fbdeal_id，
                // 事件类型fopt_type
                 **/
                Map<Tuple9<Long,String,String,String,Long,Long,Long,Long,Long>,Map<String,String>> abtablemap= new HashMap();


                Map<String,String> aTable_schema= new HashMap();
                Map<String,String> aTable_map= new HashMap();

                Map<String,String> bTable_schema= new HashMap();

                Map<String,String> abTable_relation_map = new HashMap<String, String>();
                Map<String,String> fdeal_time = new TreeMap<>();



                /**
                 * A,B两张表都有的字段：info:databaseName
                 *info:optTime
                 *info:optType
                 *info:selfTimeSequence
                 *info:tableName
                 */
                if(aTable.equals(tablename)){
                    for(String event_time:event_time_map.keySet()){
                        fdeal_time.put(event_time,jsonData.getString(event_time_map.get(event_time)));
                    }

                    String a_table_key=jsonData.getString("Fdeal_id")
                            +'_'+jsonData.getString("Fbdeal_id")
                            +'_'+jsonData.getString("Ftenancy_id")
                            +'_'+jsonData.getString("Fbuyer_id");

                    for(String key:jsonData.keySet()){
                        aTable_schema.put(key,aTable);
                    }
                    aTable_schema.remove("optTime");
                    aTable_schema.remove("optType");
                    aTable_schema.remove("selfTimeSequence");
                    aTable_schema.remove("tableName");
                    aTable_schema.remove("databaseName");


                    for(String key:jsonData.keySet()){
                        if(aTable_schema.containsKey(key))
                        aTable_map.put(key,jsonData.getString(key));
                    }


                    StringBuffer result = new StringBuffer();
                    result.append(jsonData.toJSONString()+"\t");

                    List<List<HbaseCell>> listrow=  hbaseUtil.prefixFilter(bTable,columnFamilys[0],a_table_key);
                    //如果A表关联上B表,如果区分update和insert,;如果关联不上，直接写入HBase
                    if(listrow.size()>0) {
                        for (HbaseCell hbasecell : listrow.get(0)) {
                            bTable_schema.put(hbasecell.getColumn(),bTable);
                        }
                        bTable_schema.remove("optTime");
                        bTable_schema.remove("optType");
                        bTable_schema.remove("selfTimeSequence");
                        bTable_schema.remove("tableName");
                        bTable_schema.remove("databaseName");
                        bTable_schema.remove("Fdeal_id");
                        bTable_schema.remove("Fbdeal_id");
                        bTable_schema.remove("Ftenancy_id");
                        bTable_schema.remove("Fbuyer_id");

                            for (List<HbaseCell> celllist : listrow) {
                                Map<String,String> bTable_map= new HashMap();
                                for (HbaseCell hbasecell : celllist) {

                                    if(bTable_schema.containsKey(hbasecell.getColumn())){
                                        bTable_map.put(hbasecell.getColumn(), (String)hbasecell.getValue());
                                    }
                                    //System.out.print(hbasecell.getColumn()+"："+hbasecell.getValue()+" ");
                                    result.append(hbasecell.getColumn() + "：" + hbasecell.getValue() + " ");
                                }



                                if (optType.equals("INSERT")) {
                                    /**
                                     * 如果为insert，下单、支付、出库、出库、签收时间相同的话，几个事件都写入
                                     */

                                 for(String event_type:event_type_map.keySet()){
                                    String fdeal_gen_time=jsonData.getString(event_time_map.get("fdeal_gen"));
                                    if(fdeal_gen_time.equals(jsonData.getString(event_time_map.get(event_type)))){
                                        Tuple9<Long,String,String,String,Long,Long,Long,Long,Long> key =new Tuple9();
                                        key.setField(jsonData.getLong("Ftenancy_id"),0);
                                        key.setField(fdeal_gen_time.substring(0,9),1);
                                        key.setField(jsonData.getString("Fbuyer_id"),2);
                                        key.setField(fdeal_gen_time,3);
                                        key.setField(bTable_map.get("Ftrade_id"),4);
                                        key.setField(jsonData.getString("Fdeal_id"),5);
                                        key.setField(bTable_map.get("Fitem_sku_id"),6);
                                        key.setField(jsonData.getString("Fbdeal_id"),7);
                                        key.setField(event_type_map.get(event_type),8);
                                        bTable_map.putAll(aTable_map);
                                        abtablemap.put(key,bTable_map);
                                        System.out.println( key.toString()+"**************"+abtablemap.toString());
                                    }
                                }
                               }else {
                                    //不是下单事件,支付、出库、出库、签收时间和下单事件比较,取最大更新相应事件
                                    //取最大事件
                                    Map.Entry<String, String> max_time=fdeal_time.entrySet().stream()
                                            .max((e1,e2)->e1.getValue().compareTo(e2.getValue())).get();
                                    if(max_time.getValue().compareTo(jsonData.getString(event_time_map.get("fdeal_gen")))>0){
                                        String event_type=event_type_map.get(max_time.getKey());
                                        Tuple9<Long,String,String,String,Long,Long,Long,Long,Long> key =new Tuple9();
                                        key.setField(jsonData.getLong("Ftenancy_id"),0);
                                        key.setField(max_time.getValue().substring(0,9),1);
                                        key.setField(jsonData.getString("Fbuyer_id"),2);
                                        key.setField(max_time.getValue(),3);
                                        key.setField(bTable_map.get("Ftrade_id"),4);
                                        key.setField(jsonData.getString("Fdeal_id"),5);
                                        key.setField(bTable_map.get("Fitem_sku_id"),6);
                                        key.setField(jsonData.getString("Fbdeal_id"),7);
                                        key.setField(event_type_map.get(event_type),8);
                                        bTable_map.putAll(aTable_map);
                                        abtablemap.put(key,bTable_map);
                                        System.out.println( key.toString()+"**************"+abtablemap.toString());
                                    }

                                }
                        }

                    }


                }else {
                    /**
                     * B insert 直接丢弃；B如果是update，B先和HBase B比较opt_time时间比较早，数据丢；，
                     * 如果晚的话，更新HBase，B去关联A,写入kudu
                     */
                    if(optType.equals("UPDATE")){


                        String b_table_key=jsonData.getString("Fdeal_id")
                                +'_'+jsonData.getString("Fbdeal_id")
                                +'_'+jsonData.getString("Ftenancy_id")
                                +'_'+jsonData.getString("Fbuyer_id")
                                +'_'+jsonData.getString("Ftrade_id");

                        String a_table_key=jsonData.getString("Fdeal_id")
                                +'_'+jsonData.getString("Fbdeal_id")
                                +'_'+jsonData.getString("Ftenancy_id")
                                +'_'+jsonData.getString("Fbuyer_id");


                        String optTime=jsonData.getString("optTime");

                        Map<String, String> bTable_map = new HashMap();

                        for(String event_time:event_time_map.keySet()){
                            fdeal_time.put(event_time,jsonData.getString(event_time_map.get(event_time)));
                        }

                        if (optTime.compareTo(bTable_map.get("optTime"))>0){
                            List<List<HbaseCell>> aTablelistrow=  hbaseUtil.getRow(aTable,columnFamilys[0],b_table_key);
                            for (List<HbaseCell> celllist : aTablelistrow) {
                                Tuple9<Long,String,String,String,Long,Long,Long,Long,Long> key =new Tuple9();
                                Map<String, String> aTablelook_map = new HashMap();
                                for (HbaseCell hbasecell : celllist) {
                                    aTablelook_map.put(hbasecell.getColumn(), (String)hbasecell.getValue());
                                }

                                for(String event_time:event_time_map.keySet()){
                                    fdeal_time.put(event_time,aTablelook_map.get(event_time_map.get(event_time)));
                                }

                                if("INSERT".equals(aTablelook_map.get("optType"))){
                                    /**
                                     * 如果为insert，下单、支付、出库、出库、签收时间相同的话，几个事件都写入
                                     */

                                    for(String event_type:event_type_map.keySet()){
                                        String fdeal_gen_time=jsonData.getString(event_time_map.get("fdeal_gen"));
                                        if(fdeal_gen_time.equals(jsonData.getString(event_time_map.get(event_type)))){
                                            key.setField(aTablelook_map.get("Ftenancy_id"),0);
                                            key.setField(fdeal_gen_time.substring(0,9),1);
                                            key.setField(aTablelook_map.get("Fbuyer_id"),2);
                                            key.setField(fdeal_gen_time,3);
                                            key.setField(bTable_map.get("Ftrade_id"),4);
                                            key.setField(aTablelook_map.get("Fdeal_id"),5);
                                            key.setField(bTable_map.get("Fitem_sku_id"),6);
                                            key.setField(aTablelook_map.get("Fbdeal_id"),7);
                                            key.setField(event_type_map.get(event_type),8);
                                            bTable_map.putAll(aTable_map);
                                            abtablemap.put(key,bTable_map);
                                            System.out.println( key.toString()+"**************"+abtablemap.toString());
                                        }
                                    }

                                }else {
                                    //不是下单事件,支付、出库、出库、签收时间和下单事件比较,取最大更新相应事件
                                    //取最大事件
                                    Map.Entry<String, String> max_time=fdeal_time.entrySet().stream()
                                            .max((e1,e2)->e1.getValue().compareTo(e2.getValue())).get();
                                    if(max_time.getValue().compareTo(aTablelook_map.get(event_time_map.get("fdeal_gen")))>0){
                                        String event_type=event_type_map.get(max_time.getKey());
                                        key.setField(jsonData.getLong("Ftenancy_id"),0);
                                        key.setField(max_time.getValue().substring(0,9),1);
                                        key.setField(aTablelook_map.get("Fbuyer_id"),2);
                                        key.setField(max_time.getValue(),3);
                                        key.setField(aTablelook_map.get("Ftrade_id"),4);
                                        key.setField(jsonData.getString("Fdeal_id"),5);
                                        key.setField(aTablelook_map.get("Fitem_sku_id"),6);
                                        key.setField(jsonData.getString("Fbdeal_id"),7);
                                        key.setField(event_type_map.get(event_type),8);
                                        bTable_map.putAll(aTablelook_map);
                                        abtablemap.put(key,bTable_map);
                                        System.out.println( key.toString()+"**************"+abtablemap.toString());
                                    }

                                }

                            }
                        }



                    }

                }

                return  jsonData;
            }
        });


        //ABTableFlatMapStream.print();
        env.execute();

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

    public String getAnorelation() {
        return anorelation;
    }

    public void setAnorelation(String anorelation) {
        this.anorelation = anorelation;
    }

    public static int getPrintNum() {
        return printNum;
    }
    /**
     * 字段转换
     * @param messages
     * @return
     */
    protected JSONArray parserData(String messages) {

        JSONArray datas = JSONArray.parseArray(messages);

        JSONArray dataArray = new JSONArray();

        for(int i = 0;i < datas.size();i++){
            String str = datas.getString(i).replace("\n\t", "");
            JSONObject content = JSON.parseObject(str);
            dataArray.add(content);
        }
        return dataArray;

    }



}


