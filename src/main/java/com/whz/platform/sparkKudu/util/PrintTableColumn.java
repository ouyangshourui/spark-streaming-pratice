package com.whz.platform.sparkKudu.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class PrintTableColumn {
	private static KuduContext kuduContext;
	private static KuduTable kuduTable;
	private static JavaSparkContext javaSparkContext;
	private static JavaStreamingContext context;
	
	public static void main(String[] args) throws KuduException {
		String tableName = "event.event_user_user";
		//初始化JavaStreamingContext JavaSparkContext SparkSession
		intStreamingContext("sadasd");
        //初始化kudu对象
        List<Map<String,Object>> columnList = initKudu(javaSparkContext, tableName);
        int index = 0;
        String result = "";
        String keys = "";
        for(Map<String, Object> column : columnList){
        	String key = column.get("column").toString();
//        	if(index == 0){
//        		result += "if(\"" + key + "\".equals(columnName)){\n";
//        		keys += key +",\n";
//        	}else{
//        		result += "}else if(\"" + key + "\".equals(columnName)){\n";
//        		keys += key +"\n";
//        	}
//    		result += "\tString value = StringUtils.getVolidString(jsonData.getString(\"\"));\n";
//    		result += "\tmappingData.put(columnName, value);\n";
        	
        	result += "mappingData.put(\""+ key +"\", StringUtils.getVolidString(jsonData.getString(\"\")));\n";
        	index++;
        }
//        result += "}";
        System.out.println(result);
        
        System.out.println(keys);
	}
	
	/**
	 * 初始化kudu链接对象
	 * @param context
	 * @return 
	 * @return 
	 * @throws KuduException 
	 */
	private static List<Map<String,Object>> initKudu(JavaSparkContext context,String tableName) throws KuduException{
		String kuduMaster = ResourcesUtil.getValue("conf", "kudu.master");
		System.out.println(kuduMaster);
		kuduContext = new KuduContext(kuduMaster, context.sc());
		System.out.println("kudu 初始化成功！");
		 
		kuduTable = kuduContext.syncClient().openTable(tableName);
		List<ColumnSchema> columns = kuduTable.getSchema().getColumns();
		System.out.println("kudu table[" + tableName + "] 初始化成功！");
		

		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        for (ColumnSchema schema : columns) {
        	Map<String,Object> map = new HashMap<String,Object>();
            map.put("column", schema.getName());
            map.put("type", schema.getType());
            list.add(map);
        }

        System.out.println("kudu table[" + tableName + "] structType 初始化成功！");
        
        return list;
	}
	
	/**
	 * 初始化JavaStreamingContext
	 * @param context
	 * @return 
	 */
	private static void intStreamingContext(String appname){
		SparkConf sparkConf = null;
		String runModel = ResourcesUtil.getValue("conf", "run_model");
		if(runModel.equals("local")){
			sparkConf = new SparkConf().setMaster("local").setAppName(appname);
	        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");//确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
		}else{
			sparkConf = new SparkConf().setAppName(appname);
	        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");//确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
		}
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"); 
        context = new JavaStreamingContext(sparkConf, Seconds.apply(6));
        
        System.out.println("JavaStreamingContext 初始化成功！");
        
        javaSparkContext = new JavaSparkContext(context.ssc().sc());
        System.out.println("JavaSparkContext 初始化成功！");
        
        System.out.println("SparkSession 初始化成功！");
	}

}
