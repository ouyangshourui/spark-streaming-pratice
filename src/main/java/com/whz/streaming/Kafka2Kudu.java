package com.whz.streaming;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
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
import com.whz.platform.sparkKudu.util.HttpClientUtil;

import kafka.serializer.StringDecoder;

public class Kafka2Kudu {

	private static KuduContext kuduContext;
	private static KuduTable kuduTable;
	private static JavaSparkContext javaSparkContext;
	private static JavaStreamingContext context;
	private static SparkSession sqlCtx;
	private static StructType structType;
	
	public static void main(String[] args) throws InterruptedException, IOException {
		//初始化参数
		Set<String> keys = new HashSet<String>();//所有需要存储的key
		Set<String> notNullColumns = new HashSet<String>();//不为空的列
		Map<String,String> notNullColumnKeyMap = new HashMap<String,String>();//不为空的列
		String[] calColumn = {"_event_id","_dt","_years","_year_quarter","_year_month","_year_week","_y_m_tenofmonth","_hours"};//衍生计算列
		
		//初始化参数
		String params = args[0];
		if(params == null || "".equals(params)){
			throw new RuntimeException("参数为传递");
		}
		
		String jsonParam = HttpClientUtil.get(ResourcesUtil.getValue("conf", "k-eap-jarparam") + params);
		JSONObject paramResponse = JSONObject.parseObject(jsonParam);
	
		JSONObject paramJson = paramResponse.getJSONObject("data");
		System.out.println("paramJson:" + paramJson);
		String APPNAME = paramJson.getString("appName");
		if(APPNAME == null || "".equals(APPNAME)){
			throw new RuntimeException("作业名称不能为空");
		}
		String TOPIC = paramJson.getString("topic");
		if(TOPIC == null || "".equals(TOPIC)){
			throw new RuntimeException("kafka数据源topic不能为空");
		}
		String TABLENAME = paramJson.getString("kuduTable").toLowerCase();
		if(TABLENAME == null || "".equals(TABLENAME)){
			throw new RuntimeException("kudu目标表名不能为空");
		}
		JSONArray configs = paramJson.getJSONArray("configs");
		if(configs == null || configs.size() == 0){
			throw new RuntimeException("传入的配置信息有误！");
		}
		final String[] topicContent = TOPIC.split("#");
		System.out.println("topicContent:" + TOPIC);
		if(topicContent.length > 1){
			TOPIC = topicContent[0];
			System.out.println("TOPIC:" + TOPIC);
		}
		String _platform_num = getPlatform_num(configs);
		
		//初始化JavaStreamingContext JavaSparkContext SparkSession
		intStreamingContext(APPNAME);
        //获取kafka数据流
        JavaPairInputDStream<String, String> stream = intiKafka(context, TOPIC);
        //初始化kudu对象
        List<Map<String,Object>> columnList = initKudu(javaSparkContext, TABLENAME, notNullColumns);
        //初始化计算需要的配置
        for(int i = 0;i < configs.size();i++){
			JSONArray mappingArray = configs.getJSONObject(i).getJSONArray("mappping");
			for(int j = 0;j < mappingArray.size();j++){
				JSONObject mapping = mappingArray.getJSONObject(j);
				String target = mapping.getString("target");
				keys.add(target);
				//如果当前key notnull
				if(notNullColumns.contains(target)){
					//如果有默认值 跳过
					String defaultValue = mapping.getString("defaultvalue");
					if(defaultValue != null && !"".equals(defaultValue)){
						continue;
					}
					//列是否是计算列
					if(isCalColumn(target,calColumn)){
						continue;
					}
					notNullColumnKeyMap.put(target, mapping.getString("source"));
				}
			}
		}
        System.out.println("非空column：" + notNullColumnKeyMap);
        //提取有用数据内容
        JavaDStream<Object> arrayDStream = stream.flatMap(tuple2 -> {
        	if(topicContent.length > 1){
    			try {
    				JSONArray message = JSONArray.parseArray(tuple2._2);
    				JSONArray dataArray = new JSONArray();
    				
    				for(int i = 0;i < message.size();i++){
    					JSONObject content = message.getJSONObject(i).getJSONObject("content");
    					
    					JSONArray jsonData = content.getJSONArray("data");
    					String platformNum = content.getString("platformNum");
    					String type = content.getString("type");
    					
    					for(int j = 0;j < jsonData.size();j++){
    						JSONObject data = jsonData.getJSONObject(j);
    						
    						data.put(_platform_num, platformNum);
    						data.put("_cal_type_sel", type);
    						
    						dataArray.add(data);
    					}
    				}
    				return dataArray.iterator();
    			} catch (Exception e) {
    				try {
    					JSONArray dataArray = new JSONArray();
    					
    					JSONObject message = JSONObject.parseObject(tuple2._2);
    					JSONObject content = message.getJSONObject("content");
    					
    					JSONArray jsonData = content.getJSONArray("data");
    					String platformNum = content.getString("platformNum");
    					String type = content.getString("type");
    					for(int i = 0;i < jsonData.size();i++){
    						JSONObject data = jsonData.getJSONObject(i);
    						
    						data.put(_platform_num, platformNum);
    						data.put("_cal_type_sel", type);
    						
    						dataArray.add(data);
    					}
    					
    					return dataArray.iterator();
    				} catch (Exception e1) {
    					System.out.println("对象转换异常");
    					e1.printStackTrace();
    					System.out.println(e1.getMessage());
    					JSONArray datas = new JSONArray();
    					return datas.iterator();
    				}
    			}
        	}else{
    			try {
    				JSONArray datas = JSONArray.parseArray(tuple2._2);
    				JSONArray result = new JSONArray();
    				for(int i = 0;i < datas.size();i++){
    					JSONObject content = datas.getJSONObject(i).getJSONObject("content");
    					content.put(_platform_num, datas.getJSONObject(i).getString(_platform_num));
    					result.add(content);
    				}
    				return result.iterator();
    			} catch (Exception e) {
    				e.printStackTrace();
    				try {
    					JSONArray datas = new JSONArray();
    					
    					JSONObject data = JSONObject.parseObject(tuple2._2);
    					JSONObject content = data.getJSONObject("content");
    					content.put(_platform_num, data.getString(_platform_num));
    					datas.add(content);
    					return datas.iterator();
    				} catch (Exception e1) {
    					System.out.println("对象转换异常");
    					e1.printStackTrace();
    					System.out.println(e1.getMessage());
    					JSONArray datas = new JSONArray();
    					
    					return datas.iterator();
    				}
    			}
        	
        	}
        });
        
        //垃圾数据过滤
        JavaDStream<Object> filterDStream = arrayDStream.filter(new Function<Object, Boolean>() {
        	private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Object data) throws Exception {
				JSONObject jsonData = (JSONObject)data;
				if(topicContent.length > 1){
					String type = jsonData.getString("_cal_type_sel");
					if(!topicContent[1].equals(type)){
						return false;
					}
					String pnum = jsonData.getString(_platform_num);
					if(!topicContent[2].equals(pnum)){
						return false;
					}
				}
				for(String key : notNullColumnKeyMap.keySet()){
					String value = jsonData.getString(notNullColumnKeyMap.get(key));
					if(value == null || "".equals(value)){
						System.out.println("数据合法性判断：key : " + key + " value : " + value);
						return false;
					}
				}

				return true;
			}
        });
        
       //处理数据
        JavaDStream<Object[]> dataDStream = filterDStream.flatMap(tuple2 -> {
           JSONObject jsonData = (JSONObject)tuple2;
           List<Object[]> list = new ArrayList<Object[]>();
           if(tuple2 == null){
        	   list.iterator();
           }
           //判断当前记录符合哪些事件id
           List<JSONObject> eventIdsJson = getEventId(jsonData, configs);
           for(JSONObject eventidJson : eventIdsJson){
        	   Object[] result = new Object[columnList.size()];
        	   JSONArray columnsArray = eventidJson.getJSONArray("mappping");
        	   
        	   JSONObject foptTomeMapping =  getMessageKeyByColumn(columnsArray,"_fopt_time");
        	   String _fopt_time = null;
        	   if(foptTomeMapping == null){
        		   _fopt_time = null;
        	   }else{
        		   _fopt_time = jsonData.getString(foptTomeMapping.getString("source"));
        	   }
        	   boolean isVaild = true;
        	   for(int i = 0;i < columnList.size();i++){
        		   String columnName = columnList.get(i).get("column").toString();
        		   //判断当期kafkakey是否需要获取
        		   boolean isValidKey = keys.contains(columnName);
        		   Object value = null;
        		   if(isValidKey){
        			  //获取对应事件的默认值
        			  JSONObject mapping =  getMessageKeyByColumn(columnsArray,columnName);
            		  String defaultValue = mapping.getString("defaultvalue");
            		  if(defaultValue == null || defaultValue.equals("")){
            			  value = jsonData.getString(mapping.getString("source"));
            		  }else{
            			  value = defaultValue;
            		  }
        		   }
        		   //为计算列赋值
        		   if(value == null || value.equals("")){
        			   try {
						value = mkCalColumn(columnName,_fopt_time,eventidJson);
        			   } catch (Exception e) {
						value = "";
						isVaild = false;
						System.out.println("转换错误：" + e.getMessage());
					}
        		   }
        		   
        		   //转换value值类型
        		   result[i] = changeValueType(value,(Type)columnList.get(i).get("type"));
         	   }
        	   
        	   //添加至返回值
        	   if(isVaild){
        		   list.add(result);
        	   }
           }
           return list.iterator();
        });
        
        //转换数据
        JavaDStream<Row> pairDStream = dataDStream.map(tuple2 -> {
        	if(tuple2 == null){
        		return null;
        	}
        	Object[] data = (Object[])tuple2;
      	   //判断数据 那种类型的数据
      	   
      	   return RowFactory.create(data);
        });
        
        //存储数据
        pairDStream.foreachRDD(it ->{
        	listSaveHandler(it,TABLENAME);
        });
        context.start();
        context.awaitTermination();
	}
	
	/**
	 * 
	 * @param tuple2
	 * @param tABLENAME2 
	 * @return
	 */
	public static void listSaveHandler(Object tuple2,String TABLENAME){
		if(tuple2 == null){
			System.out.println("数据异常！！！");
			return;
		}
		JavaRDD<Row> result = (JavaRDD<Row>) tuple2;
		
		Dataset<Row> dataset = sqlCtx.createDataFrame(result, structType);
		
		kuduContext.upsertRows(dataset, TABLENAME);
   }
	
	private static String getPlatform_num(JSONArray configs) {
		JSONArray mappingArray = configs.getJSONObject(0).getJSONArray("mappping");
		for(int j = 0;j < mappingArray.size();j++){
			JSONObject mapping = mappingArray.getJSONObject(j);
			String target = mapping.getString("target");
			if(target.equals("_ftenancy_id")){
				return mapping.getString("source");
			}
		}
		return null;
	}

	/**
	 * 为计算列赋值
	 * @param columnName
	 * @param _fopt_time
	 * @param eventidJson
	 * @throws ParseException 
	 */
	private static Object mkCalColumn(String columnName,String _fopt_time,JSONObject eventidJson) throws ParseException{
		Object value = null;
		if("_event_id".equals(columnName)){
			value = eventidJson.getString("eventId");
		}
		if(_fopt_time == null){
//			System.out.println("_fopt_time为空 直接返回");
			return null;
		}
		if("_dt".equals(columnName)){
			value = _fopt_time.split(" ")[0];
		}else if("_years".equals(columnName)){
			value = _fopt_time.split(" ")[0].split("\\-")[0];
		}else if("_year_quarter".equals(columnName)){
			String[] d = _fopt_time.split(" ")[0].split("\\-");
			int month = Integer.valueOf(d[1]);
			if(month < 4){
				value = d[0] + "-" + "Q1";
			}else if(month < 7){
				value = d[0] + "-" + "Q2";
			}else if(month < 10){
				value = d[0] + "-" + "Q3";
			}else if(month < 13){
				value = d[0] + "-" + "Q4";
			}
		}else if("_year_month".equals(columnName)){
			String[] d = _fopt_time.split(" ")[0].split("\\-");
			if(d[1].length() > 1){
				value = d[0] + "-" + d[1];
			}else{
				value = d[0] + "-0" + d[1];
			}
		}else if("_year_week".equals(columnName)){
			value = getSeqWeek(_fopt_time);
		}else if("_y_m_tenofmonth".equals(columnName)){
			String[] d = _fopt_time.split(" ")[0].split("\\-");
			int day = Integer.valueOf(d[2]);
			String month;
			if(d[1].length() > 1){
				month = d[1];
			}else{
				month = "0" + d[1];
			}
			if(day < 11){
				value = d[0] + "-" + month + "-" + "T1";
			}else if(day < 21){
				value = d[0] + "-" + month + "-" + "T2";
			}else if(day < 32){
				value = d[0] + "-" + month + "-" + "T3";
			}
		}else if("_hours".equals(columnName)){
			String hour = _fopt_time.split(" ")[1].split(":")[0];
			if(hour.length() > 1){
				value = hour;
			}else{
				value = "0" + hour;
			}
		}
		return value;
	}
	
	/**
	 * 获取事件id
	 * @param jsonData
	 * @return
	 */
	private static List<JSONObject> getEventId(JSONObject jsonData,JSONArray configs){
		List<JSONObject> eventids = new ArrayList<JSONObject>();
		for(int i = 0;i < configs.size();i++){
 		  String condition = configs.getJSONObject(i).getString("condition");
 		  if(condition == null){
 			 eventids.add(configs.getJSONObject(i));
 			 continue;
 		  }
		  String[] conditions = condition.split("_");
		  //获取数据里面的data
		  String data = jsonData.getString(conditions[0]);
		  if(conditions.length == 1){
			  eventids.add(configs.getJSONObject(i));
		  }else if(conditions[1].equals("=")){
			  if(conditions[2].equals(data)){
				  eventids.add(configs.getJSONObject(i));
			  }
		  }else if(conditions[1].equals(">")){
			  if(conditions[2].compareTo(data) == -1){
				  eventids.add(configs.getJSONObject(i));
			  }
		  }else if(conditions[1].equals("<")){
			  if(conditions[2].compareTo(data) == 1){
				  eventids.add(configs.getJSONObject(i));
			  }
		  }
		  
 	   }
		if(eventids.size() == 0){
			System.out.println("eventids:" + eventids);
		}
		return eventids;
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
	 * 根据表字段 获取对应的 kafka消息体字段
	 * @param columnsArray
	 * @param column
	 * @return
	 */
	private static JSONObject getMessageKeyByColumn(JSONArray columnsArray,String column){
		for(int i = 0; i < columnsArray.size(); i++){
			JSONObject mapping = columnsArray.getJSONObject(i);
			if(mapping.getString("target").equalsIgnoreCase(column)){
				return mapping;
			}
		}
		return null;
	}
	
	/**
	 * 判断列是否是计算列
	 * @param target
	 * @return
	 */
	private static boolean isCalColumn(String target,String[] calColumn) {
		for (int i = 0; i < calColumn.length; i++) {
			if(target.equals(calColumn[i])){
				return true;
			}
		}
		return false;
	}

	/**
	 * 初始化JavaStreamingContext
	 * @param context
	 * @return 
	 */
	private static void intStreamingContext(String APPNAME){
		SparkConf sparkConf = null;
		String runModel = ResourcesUtil.getValue("conf", "run_model");
		if(runModel.equals("local")){
			sparkConf = new SparkConf().setMaster("local").setAppName(APPNAME);
	        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");//确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
		}else{
			sparkConf = new SparkConf().setAppName(APPNAME);
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
	private static JavaPairInputDStream<String, String> intiKafka(JavaStreamingContext context,String TOPIC){
		// 首先要创建一份kafka参数map
        Map<String, String> kafkaParams = new HashMap<>();
        String broker = ResourcesUtil.getValue("conf", "metadata.broker.list");
        String offsetReset = ResourcesUtil.getValue("conf", "auto.offset.reset");
        String group = ResourcesUtil.getValue("conf", "group.id");
        // 这里是不需要zookeeper节点,所以这里放broker.list
        kafkaParams.put("metadata.broker.list",broker);
        kafkaParams.put("auto.offset.reset", offsetReset);
        kafkaParams.put("group.id", group );
        
        Set<String> topicSet = new HashSet<String>();
        topicSet.add(TOPIC);
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
	private static List<Map<String,Object>> initKudu(JavaSparkContext context,String TABLENAME,Set<String> notNullColumns) throws KuduException{
		kuduContext = new KuduContext(ResourcesUtil.getValue("conf", "kudu.master"), context.sc());
		System.out.println("kudu 初始化成功！");
		 
		kuduTable = kuduContext.syncClient().openTable(TABLENAME);
		List<ColumnSchema> columns = kuduTable.getSchema().getColumns();
		System.out.println("kudu table[" + TABLENAME + "] 初始化成功！");
		
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
        
        System.out.println("kudu table[" + TABLENAME + "] structType 初始化成功！");
        
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
