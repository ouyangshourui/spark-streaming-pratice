package com.whz.streaming;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.whz.platform.sparkKudu.util.ResourcesUtil;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import kafka.serializer.StringDecoder;

public class Kafka2KuduSku {

	private static KuduContext kuduContext;
	private static KuduTable kuduTable;
	private static JavaSparkContext javaSparkContext;
	private static JavaStreamingContext context;
	private static SparkSession sqlCtx;
	private static StructType structType;
	
	public static void main(String[] args) throws InterruptedException, IOException {
		final String appname = "event_baby_store_sku_dim";
		final String topic = "Baby-DataSys-0002";
		final String groupId = "Baby-DataSys-0002-2";
		final String tablename = "event.event_baby_store_sku_dim";
		final int validType = 2;
		
		Set<String> notNullColumns = new HashSet<String>();//不为空的列
		//初始化JavaStreamingContext JavaSparkContext SparkSession
		intStreamingContext(appname);
        //获取kafka数据流
        JavaPairInputDStream<String, String> stream = intiKafka(context, topic,groupId);
        //初始化kudu对象
        List<Map<String,Object>> columnList = initKudu(javaSparkContext, tablename,notNullColumns);
        
        //提取有用数据内容
        JavaDStream<Object> arrayDStream = stream.flatMap(tuple2 -> {
			try {
				JSONArray datas = JSONArray.parseArray(tuple2._2);
				
				JSONArray result = parserKafkaData(datas,validType);
				
				return result.iterator();
			} catch (Exception e) {
				System.out.println("数组 转换异常");
				e.printStackTrace();
				JSONArray datas = new JSONArray();
				
				return datas.iterator();
			}
        });
        
        JavaDStream<Object> tranfDStream = arrayDStream.map(tuple -> {
        	JSONObject jsonData = (JSONObject)tuple;
        	if(jsonData == null)return jsonData;
        	JSONObject mappingData = mappingData(columnList,jsonData);
        	setDefaultColumn(mappingData);
        	return mappingData;
        });
        
        //垃圾数据过滤
        JavaDStream<Object> filterDStream = tranfDStream.filter(new Function<Object, Boolean>() {
        	private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Object data) throws Exception {
				JSONObject jsonData = (JSONObject)data;

				return isValidData(jsonData,notNullColumns);
			}
        });
        
       //处理数据
        JavaDStream<Row> dataDStream = filterDStream.map(tuple2 -> {
           JSONObject jsonData = (JSONObject)tuple2;
           //判断当前记录符合哪些事件id
    	   Object[] result = getData(jsonData,columnList);
    	   
    	   return RowFactory.create(result);
        });
        
       //存储数据
        dataDStream.foreachRDD(it ->{
       		listSaveHandler(it,tablename);
       });
        
        context.start();
        context.awaitTermination();
	}
	
	private static JSONObject mappingData(List<Map<String, Object>> columnList, JSONObject jsonData) throws ParseException {
		JSONObject mappingData = new JSONObject();
		for(Map<String, Object> column : columnList){
			String columnName = column.get("column").toString();
			if("k_ftenancy_id".equals(columnName)){
				mappingData.put(columnName, jsonData.get("cal_platformNum"));
			}else if("skuno".equals(columnName)){
				String value = jsonData.getString("skuNo");
				mappingData.put(columnName, value);
			}else if("skuname".equals(columnName)){
				String value = jsonData.getString("skuName");
				mappingData.put(columnName, value);
			}else if("wholesaleprice".equals(columnName)){
				String value = jsonData.getString("wholesalePrice");
				mappingData.put(columnName, value);
			}else if("vipprice".equals(columnName)){
				String value = jsonData.getString("vipPrice");
				mappingData.put(columnName, value);
			}else if("unit".equals(columnName)){
				String value = jsonData.getString("unit");
				mappingData.put(columnName, value);
			}else if("standard".equals(columnName)){
				String value = jsonData.getString("standard");
				mappingData.put(columnName, value);
			}else if("skubarcode".equals(columnName)){
				String value = jsonData.getString("skuBarcode");
				mappingData.put(columnName, value);
			}else if("sellprice".equals(columnName)){
				String value = jsonData.getString("sellPrice");
				mappingData.put(columnName, value);
			}else if("catename3".equals(columnName)){
				String value = jsonData.getString("cateName3");
				mappingData.put(columnName, value);
			}else if("catename2".equals(columnName)){
				String value = jsonData.getString("cateName2");
				mappingData.put(columnName, value);
			}else if("catename1".equals(columnName)){
				String value = jsonData.getString("cateName1");
				mappingData.put(columnName, value);
			}else if("catecode3".equals(columnName)){
				String value = jsonData.getString("cateCode3");
				mappingData.put(columnName, value);
			}else if("catecode2".equals(columnName)){
				String value = jsonData.getString("cateCode2");
				mappingData.put(columnName, value);
			}else if("catecode1".equals(columnName)){
				String value = jsonData.getString("cateCode1");
				mappingData.put(columnName, value);
			}else if("buyprice".equals(columnName)){
				String value = jsonData.getString("buyPrice");
				mappingData.put(columnName, value);
			}else if("brandname".equals(columnName)){
				String value = jsonData.getString("brandName");
				mappingData.put(columnName, value);
			}else if("brandcode".equals(columnName)){
				String value = jsonData.getString("brandCode");
				mappingData.put(columnName, value);
			}
		}
		return mappingData;
	}
	private static void setDefaultColumn(JSONObject mappingData) {
		mappingData.put("etl_date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
	}
	
	private static Object[] getData(JSONObject jsonData,List<Map<String,Object>> columnList) {
		Object[] result = new Object[columnList.size()];
		for(int i = 0;i < columnList.size();i++){
			String columnName = columnList.get(i).get("column").toString();
			Object value = jsonData.getString(columnName);
			
			try {
				result[i] = changeValueType(value,(Type)columnList.get(i).get("type"));
			} catch (Exception e) {
				result[i] = null;
			}
		}
		return result;
	}

	protected static Boolean isValidData(JSONObject jsonData,Set<String> notNullColumns) {
		if(jsonData == null){
			System.out.println("数据为null直接返回");
			return false;
		}
		for(String key : notNullColumns){
			String value = jsonData.getString(key);
			if(value == null || "".equals(value)){
				System.out.println("数据合法性判断：key : " + key + " value : " + value);
				return false;
			}
		}
		return true;
	}

	private static JSONArray parserKafkaData(JSONArray datas,int validType) {
		JSONArray dataArray = new JSONArray();
		
		for(int i = 0;i < datas.size();i++){
			JSONObject content = datas.getJSONObject(i).getJSONObject("content");
			
			JSONArray jsonData = content.getJSONArray("data");
			String platformNum = content.getString("platformNum");
			int type = content.getIntValue("type");
			if(validType == type){
				for(int j = 0;j < jsonData.size();j++){
					JSONObject data = jsonData.getJSONObject(j);
					
					data.put("cal_platformNum", platformNum);
					dataArray.add(data);
				}
			}
			
		}
		return dataArray;
	}

	public static void listSaveHandler(Object tuple2,String tableName){
		
		JavaRDD<Row> result = (JavaRDD<Row>) tuple2;
		
		Dataset<Row> dataset = sqlCtx.createDataFrame(result, structType);
		
		kuduContext.upsertRows(dataset, tableName);
   }
	
	/**
	 * 产生周序列,即得到当前时间所在的年度是第几周
	 * @return String	  
	 * @throws ParseException */
	public static String getSeqWeek(String date) throws ParseException {
		try {
			Calendar c = Calendar.getInstance();
			c.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date));
			String week = Integer.toString(c.get(Calendar.WEEK_OF_YEAR));
			if(week.length() > 1){
				week = "W" + week;
			}else{
				week = "W0" + week;
			}
			String year = Integer.toString(c.get(Calendar.YEAR));
			return year + "-" + week;
		} catch (Exception e) {
			System.out.println("日期类型有误：" + date);
			return "";
		}
	} 
	
	/**
	 * 转换数据类型
	 * @param value
	 * @param columnSchema
	 * @return
	 */
	private static Object changeValueType(Object value, Type type) {
		switch (type) {
	        case BOOL:
	        	if(value == null || "".equals(value)){
	        		return null;
	        	}
	            return Boolean.parseBoolean(value.toString());
	        case INT8:
	        	if(value == null || "".equals(value)){
	        		return null;
	        	}
	            return Byte.valueOf(value.toString());
	        case INT16:
	        	if(value == null || "".equals(value)){
	        		return null;
	        	}
	            return Short.valueOf(value.toString());
	        case INT32:
	        	if(value == null || "".equals(value)){
	        		return null;
	        	}
	            return Integer.valueOf(value.toString());
	        case INT64:
	        	if(value == null || "".equals(value)){
	        		return null;
	        	}
	            return Long.valueOf(value.toString());
	        case FLOAT:
	        	if(value == null || "".equals(value)){
	        		return null;
	        	}
	            return Float.valueOf(value.toString());
	        case DOUBLE:
	        	if(value == null || "".equals(value)){
	        		return null;
	        	}
	            return Double.valueOf(value.toString());
	        case STRING:
	        	if(value == null || "".equals(value)){
	        		return null;
	        	}
	            return value.toString();
	        case UNIXTIME_MICROS:
	        	if(value == null || "".equals(value)){
	        		return null;
	        	}
	            return Long.valueOf(value.toString());
	        default:
	        	if(value == null || "".equals(value)){
	        		return null;
	        	}
	            return value.toString();
	    }
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
        
        sqlCtx = SparkSession.builder().config(sparkConf).getOrCreate();
        System.out.println("SparkSession 初始化成功！");
	}
    
	/**
	 * 初始化intiKafka
	 * @param context
	 * @return 
	 */
	private static JavaPairInputDStream<String, String> intiKafka(JavaStreamingContext context,String topic,String groupId){
		// 首先要创建一份kafka参数map
        Map<String, String> kafkaParams = new HashMap<>();
        String broker = ResourcesUtil.getValue("conf", "metadata.broker.list");
        String offsetReset = ResourcesUtil.getValue("conf", "auto.offset.reset");
        // 这里是不需要zookeeper节点,所以这里放broker.list
        kafkaParams.put("metadata.broker.list",broker);
        kafkaParams.put("auto.offset.reset", offsetReset);
        kafkaParams.put("group.id", groupId );        String fetchMaxBytes = ResourcesUtil.getValue("conf", "collect.fetch.message.max.bytes");

        kafkaParams.put("fetch.message.max.bytes", fetchMaxBytes);

        Set<String> topicSet = new HashSet<String>();
        topicSet.add(topic);
        System.out.println("kafka 初始化成功！");
        return KafkaUtils.createDirectStream(context, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicSet);
	}
	
	/**
	 * 初始化kudu链接对象
	 * @param context
	 * @return 
	 * @return 
	 * @throws KuduException 
	 */
	private static List<Map<String,Object>> initKudu(JavaSparkContext context,String tableName,Set<String> notNullColumns) throws KuduException{
		kuduContext = new KuduContext(ResourcesUtil.getValue("conf", "kudu.master"), context.sc());
		System.out.println("kudu 初始化成功！");
		 
		kuduTable = kuduContext.syncClient().openTable(tableName);
		List<ColumnSchema> columns = kuduTable.getSchema().getColumns();
		System.out.println("kudu table[" + tableName + "] 初始化成功！");
		
		List<StructField> structFields = new ArrayList<>();

		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        for (ColumnSchema schema : columns) {
        	Map<String,Object> map = new HashMap<String,Object>();
        	if(!schema.isNullable()){
        		notNullColumns.add(schema.getName());
        	}
            structFields.add(parse(schema));
            map.put("column", schema.getName());
            map.put("type", schema.getType());
            list.add(map);
        }

        structType = DataTypes.createStructType(structFields);
        
        System.out.println("kudu table[" + tableName + "] structType 初始化成功！");
        
        return list;
	}
	
	private static StructField parse(ColumnSchema column) {
        switch (column.getType()) {
            case BOOL:
                return DataTypes.createStructField(column.getName(), DataTypes.BooleanType, true);
            case INT8:
                return DataTypes.createStructField(column.getName(), DataTypes.ByteType, true);
            case INT16:
                return DataTypes.createStructField(column.getName(), DataTypes.ShortType, true);
            case INT32:
                return DataTypes.createStructField(column.getName(), DataTypes.IntegerType, true);
            case INT64:
                return DataTypes.createStructField(column.getName(), DataTypes.LongType, true);
            case FLOAT:
                return DataTypes.createStructField(column.getName(), DataTypes.FloatType, true);
            case DOUBLE:
                return DataTypes.createStructField(column.getName(), DataTypes.DoubleType, true);
            case STRING:
                return DataTypes.createStructField(column.getName(), DataTypes.StringType, true);
            case BINARY:
                return DataTypes.createStructField(column.getName(), DataTypes.BinaryType, true);
            case UNIXTIME_MICROS:
                return DataTypes.createStructField(column.getName(), DataTypes.LongType, true);
            default:
                return DataTypes.createStructField(column.getName(), DataTypes.StringType, true);
        }
    }
}
