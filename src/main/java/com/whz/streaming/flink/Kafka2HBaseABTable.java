package com.whz.streaming.flink;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.hbase.HBaseUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.*;

public class Kafka2HBaseABTable {

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

        Kafka2HBaseABTable kuduDealTradeTable = new Kafka2HBaseABTable();
        kuduDealTradeTable.setaTable("t_deal");
        kuduDealTradeTable.setbTable("t_trade");
        kuduDealTradeTable.setAnorelation("t_deal_norelation_t_trade");
        String aTable="t_deal";
        String bTable="t_trade";
        String anorelation="t_deal_norelation_t_trade";

        HBaseUtil hbaseInstance=HBaseUtil.getInstance();

        //全量写入A表

//        if(null!=args[1]) {
     //       if(Boolean.getBoolean(args[1])) {
                if(hbaseInstance.existTable(kuduDealTradeTable.getaTable()))
                    hbaseInstance.dropTable(kuduDealTradeTable.getaTable());
                if(hbaseInstance.existTable(kuduDealTradeTable.getbTable()))
                   hbaseInstance.dropTable(kuduDealTradeTable.getbTable());
                if(hbaseInstance.existTable(kuduDealTradeTable.getAnorelation()))
                hbaseInstance.dropTable(kuduDealTradeTable.getAnorelation());
   //       }
   //     }

        hbaseInstance.createTable(kuduDealTradeTable.getaTable(),columnFamilys);
        //全量写入B表
        hbaseInstance.createTable(kuduDealTradeTable.getbTable(),columnFamilys);
        //A表位关联上的B记录的表
        hbaseInstance.createTable(kuduDealTradeTable.getAnorelation(),columnFamilys);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<>(topic,
                new SimpleStringSchema(), properties);

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



        filterstream.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonData) throws Exception {
                HBaseUtil hbaseUtil = HBaseUtil.getInstance();
                String tablename =jsonData.getString("tableName");
                if(aTable.equals(tablename)){
                    String a_table_key=jsonData.getString("Fdeal_id")
                            +'_'+jsonData.getString("Fbdeal_id")
                            +'_'+jsonData.getString("Ftenancy_id")
                            +'_'+jsonData.getString("Fbuyer_id");

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
                            +'_'+jsonData.getString("Ftrade_id");
                    for(String key:jsonData.keySet()){
                       hbaseUtil.insertData(bTable,b_table_key,columnFamilys[0],
                                key,jsonData.getString(key));
                    }
                     //  System.out.println("write b table to hbase: "+tablename+":"+b_table_key);
                }
                return  jsonData;
            }
        });



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


