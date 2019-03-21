package com.whz.streaming.flink;

import com.whz.platform.sparkKudu.hbase.HBaseUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * spark-submit --master yarn-cluster --num-executors 5   --executor-memory 2g
 *     --class SparkReadHiveTDeal2HBase k-eap-streaming.jar 2>&1 1.txt
 */
public class SparkReadHiveTDeal2HBase {
    public static void main(String[] args) throws  Exception {
        String hbaseTable = "t_deal";
        String[] rowkeyarray = {"fdeal_id", "_fbdeal_id", "_ftenancy_id", "_fbuyer_id"};
        String hivesql = "select * from fdm.fdm_oms_orders_t_deal_chain where dp='active'";
        String hbasezookeeper = "10.254.1.153,10.254.0.81";
        HBaseUtil.setZookeeper(hbasezookeeper);
//        Map<String,Map<String,String>> rowkeydataMap = new  HashMap<>();
//        Map<String,String> data= new HashMap<>();
//        data.put("test","test");
//        rowkeydataMap.put("test",data);
//        HBaseUtil.getInstance().insertData(hbaseTable,"info", rowkeydataMap);

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkReadHiveTDeal2HBase")
                .enableHiveSupport()
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Dataset<Row> sqlDF = spark.sql(hivesql);
        String[] hiveColumnArray = sqlDF.columns();
        System.out.println("*****hive schema:"+hiveColumnArray.toString());

        Map<String, String> schemaMap = new Hashtable<>();
        for (String column : hiveColumnArray) {
            schemaMap.put(column, column);
        }

        Map<Integer, String> hbaseRowkeyMap = new Hashtable<>();
        int i = 0;
        for (String rowkey : rowkeyarray) {
            hbaseRowkeyMap.put(i, rowkey);
            i = i + 1;
        }

        Broadcast<String> broadcastHBasetable = sc.broadcast(hbaseTable);
        Broadcast<String> broadcastHBasezk = sc.broadcast(hbasezookeeper);
        Broadcast<Map<Integer, String>> broadcastRowkeyMap = sc.broadcast(hbaseRowkeyMap);
        long dfcount = sqlDF.count();

        Broadcast<Map<String, String>> broadcastSchemaMap = sc.broadcast(schemaMap);

        sqlDF.repartition((int) dfcount / 50000 + 1).foreachPartition(partition -> {
            String hbasetable = broadcastHBasetable.value();
            Map<Integer, String> rowkeysMap = broadcastRowkeyMap.value();
            String hbasezk = broadcastHBasezk.value();
            Map<String, String> hiveSchemapmap = broadcastSchemaMap.value();
            HBaseUtil.setZookeeper(hbasezk);
            String rowkeys = "";
            HBaseUtil.getInstance().getTableHTable(hbasetable);

            Map<String, String> dataMap = new HashMap();
            Map<String,Map<String,String>> rowkeydataMap = new  HashMap<>();
            while (partition.hasNext()) {
                rowkeys = "";
                dataMap.clear();
                Row recode = partition.next();
                // System.out.println("recode schema:"+recode.schema());
                // System.out.println(recode);
                for (int j = 0; j < rowkeysMap.keySet().size(); j++) {
                    if(rowkeysMap.get(j).startsWith("_"))
                        rowkeys = rowkeys +"_"+recode.getAs(rowkeysMap.get(j).substring(1));
                    else
                        rowkeys = rowkeys +recode.getAs(rowkeysMap.get(j));
                }
                for (String shemakey : hiveSchemapmap.keySet()) {
                    dataMap.put(shemakey, recode.getAs(shemakey)+"");
                }
                rowkeydataMap.put(rowkeys,dataMap);
            }
            System.out.println("rowkeys:"+rowkeys);
            System.out.println("dataMap:"+dataMap);
            System.out.println("load data to hbase....");
            HBaseUtil.getInstance().insertData(hbaseTable,"info", rowkeydataMap);
            System.out.println("load data to hbase....");
        });

        spark.close();

    }
}
