package sparkKudu;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kudu.client.KuduException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.jdbc.JdbcFactory;
import com.whz.platform.sparkKudu.jdbc.JdbcUtils;
import com.whz.platform.sparkKudu.model.JdbcModel;
import com.whz.platform.sparkKudu.util.ResourcesUtil;
import com.whz.platform.sparkKudu.util.StringUtils;

import kafka.serializer.StringDecoder;

public class Kafka2TidbScore {

	private static JavaStreamingContext context;
	
	public static void main(String[] args) throws InterruptedException, IOException {
		final String appname = "event_b2b2c_userscore_tidb";
		final String groupId = "event_b2b2c_userscore_tidb";
		final String kuduMaster = ResourcesUtil.getValue("conf", "kudu.master");
		
		final String database = "bi";
		final String tablename = "bi_user";
		
		final String topic = ResourcesUtil.getValue("conf", "collect.user.topic");
		final String validType = "t_score_new";
		
		//初始化JavaStreamingContext JavaSparkContext SparkSession
		intStreamingContext(appname);
        //获取kafka数据流
        JavaPairInputDStream<String, String> stream = intiKafka(context, topic,groupId);
        //初始化kudu对象
        JdbcModel jdbcModel = initTidbModel();
        
        final Set<String> columns = JdbcFactory.getJDBCUtil(jdbcModel).getColumnInfoList(database, tablename);
        
        //提取有用数据内容
        JavaDStream<Object> arrayDStream = stream.flatMap(tuple2 -> {
			try {
				JSONArray datas = JSONArray.parseArray(tuple2._2);
				
				JSONArray result = parserKafkaData(datas);
				
				return result.iterator();
			} catch (Exception e) {
				System.out.println("数组 转换异常");
				e.printStackTrace();
				JSONArray datas = new JSONArray();
				
				return datas.iterator();
			}
        });
        
        //过滤掉不需要的表数据
        JavaDStream<Object> tableFilterDStream = arrayDStream.filter(new Function<Object, Boolean>() {
        	private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Object data) throws Exception {
				JSONObject jsonData = (JSONObject)data;
				String tableName = jsonData.getString("tableName");
				
				Pattern pattern = Pattern.compile(validType + "(_)?[0-9]*");
			    Matcher isNum = pattern.matcher(tableName);
			    if (!isNum.matches()) {
			    	return false;
			    }
			    if(!"2".equals(jsonData.getString("fAccountType")) && !"4".equals(jsonData.getString("fAccountType"))){
			    	return false;
			    }
			    if(args.length > 0 && !"".equals(args[0])) {
			    	String optTime = jsonData.getString("optTime");
			    	if(optTime == null || "".equals(optTime)) {
			    		return false;
			    	}
			    	if(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(Long.parseLong(optTime))).compareTo(args[0]) < 0) {
			    		return false;
			    	}
			    }
				return true;
			}
        });
        JavaDStream<JSONObject> tranfDStream = tableFilterDStream.map(tuple -> {
        	JSONObject jsonData = (JSONObject)tuple;
        	if(jsonData == null)return jsonData;
        	JSONObject mappingData = mappingData(jsonData,columns,kuduMaster);
        	
        	setDefaultColumn(mappingData);
        	
        	return mappingData;
        });
        
	    //存储数据
        tranfDStream.foreachRDD(rdd -> {
        	rdd.foreachPartition(partition -> {
        		jdbcModel.setDatabaseKey(partition.hashCode() + "");
        		JdbcUtils util = JdbcFactory.getJDBCUtil(jdbcModel);
        		while(partition.hasNext()){
					listSaveHandler(partition.next(),tablename,util);
				}
        	});
        });
        
        context.start();
        context.awaitTermination();
	}
	
	private static JSONObject mappingData(JSONObject jsonData,Set<String> columns,String kuduMaster) throws ParseException {
		JSONObject mappingData = new JSONObject();
		for(String columnName : columns){
			if("k_ftenancy_id".equals(columnName)){
				String value = StringUtils.getVolidString(jsonData.getString("fTenancyId"));//fTenancyId
				mappingData.put(columnName, value);
			}else if("k_fid".equals(columnName)){
				String value = StringUtils.getVolidString(jsonData.getString("fUid"));//fUid
				mappingData.put(columnName, value);
			}else if("faccountpoints".equals(columnName)){
				int faccountType = jsonData.getIntValue("fAccountType");
				if(faccountType == 2){
					String value = StringUtils.getVolidString(jsonData.getString("fAccountPoints"));//fAccountPoints
					mappingData.put(columnName, value);
				}
			}else if("fmlevelvalue".equals(columnName)){
				int faccountType = jsonData.getIntValue("fAccountType");
				if(faccountType == 4){
					String value = StringUtils.getVolidString(jsonData.getString("fAccountPoints"));
					mappingData.put(columnName, value);
				}
			}
		}
		return mappingData;
	}
	
	private static void setDefaultColumn(JSONObject mappingData) {
		mappingData.put("etl_date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
	}
	
	private static JSONArray parserKafkaData(JSONArray datas) {
		JSONArray dataArray = new JSONArray();
		
		for(int i = 0;i < datas.size();i++){
			String str = datas.getString(i).replace("\n\t", "");
			JSONObject content = JSON.parseObject(str);
			dataArray.add(content);
		}
		return dataArray;
	}

	public static void listSaveHandler(JSONObject result,String tableName,JdbcUtils util){
		
		try {
			Object[] params = new Object[result.keySet().size() + result.keySet().size()];
			
			int index = 0;
			
			String columnSql = "";
			String valueSql = "";
			for(String column : result.keySet()){
				if("".equals(columnSql)){
					columnSql += column;
				}else{
					columnSql += "," + column;
				}
				if("".equals(valueSql)){
					valueSql += "?";
				}else{
					valueSql += ",?";
				}
				params[index] = result.get(column);
				index++;
			}
			
			String updateSql = "";
			for(String column : result.keySet()){
				if("".equals(updateSql)){
					updateSql += column + "=?";
				}else{
					updateSql += "," + column + "=?";
				}
				params[index] = result.get(column);
				index++;
			}
			
			String insertSql = "insert into " + tableName +" (" + columnSql + ") VALUES(" + valueSql + ") ON DUPLICATE KEY UPDATE " + updateSql;
			util.excuteSql(insertSql, params);
		} catch (Exception e) {
			e.printStackTrace();
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
		sparkConf.set("spark.streaming.backpressure.enabled", "true"); 
		sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100"); 
		
        context = new JavaStreamingContext(sparkConf, Seconds.apply(6));
        
        System.out.println("JavaStreamingContext 初始化成功！");
	}
    
	/**
	 * 初始化intiKafka
	 * @param context
	 * @return 
	 */
	private static JavaPairInputDStream<String, String> intiKafka(JavaStreamingContext context,String topic,String groupId){
		// 首先要创建一份kafka参数map
		Map<String, String> kafkaParams = new HashMap<>();
        String broker = ResourcesUtil.getValue("conf", "collect.metadata.broker.list");
        String offsetReset = ResourcesUtil.getValue("conf", "collect.auto.offset.reset");
        String fetchMaxBytes = ResourcesUtil.getValue("conf", "collect.fetch.message.max.bytes");
        // 这里是不需要zookeeper节点,所以这里放broker.list
        kafkaParams.put("metadata.broker.list",broker);
        kafkaParams.put("auto.offset.reset", offsetReset);
        kafkaParams.put("group.id", groupId );
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
	private static JdbcModel initTidbModel() throws KuduException{
		JdbcModel model = new JdbcModel(
				"baby-table",
				ResourcesUtil.getValue("conf", "tidb.driverClassName"),
				ResourcesUtil.getValue("conf", "tidb.url"),
				ResourcesUtil.getValue("conf", "tidb.username"),
				ResourcesUtil.getValue("conf", "tidb.password")
				);
        System.out.println("tidb 连接对象 初始化成功！");
        
        return model;
	}

}
