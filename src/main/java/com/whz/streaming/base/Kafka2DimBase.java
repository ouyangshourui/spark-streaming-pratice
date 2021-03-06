package com.whz.streaming.base;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.whz.platform.sparkKudu.jdbc.JdbcUtils;
import com.whz.platform.sparkKudu.util.KafkaUtil;
import com.whz.platform.sparkKudu.util.ResourcesUtil;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.Redis.RedisUtil;
import com.whz.platform.sparkKudu.jdbc.JdbcFactory;
import com.whz.platform.sparkKudu.kudu.KuduUtil;
import com.whz.platform.sparkKudu.model.JdbcModel;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

public abstract class Kafka2DimBase {
	
	private JavaStreamingContext context;
	private String cacheKuduMaster;
	private String appname;
	private String topic;
	private String groupId;
	private String cacheTabel;
	private String validType;
	private String broker;
	private String keyColumn;
	private String valueColumn;
	private String ftenancyColumn;
	private String[] args;
	
	public final Kafka2DimBase setKeyColumn(String keyColumn) {
		this.keyColumn = keyColumn;
		return this;
	}
	public final Kafka2DimBase setValueColumn(String valueColumn) {
		this.valueColumn = valueColumn;
		return this;
	}
	public void setFtenancyColumn(String ftenancyColumn) {
		this.ftenancyColumn = ftenancyColumn;
	}
	public final Kafka2DimBase setBroker(String broker) {
		this.broker = broker;
		return this;
	}
	public final Kafka2DimBase setAppname(String appname) {
		this.appname = appname;
		return this;
	}
	public final Kafka2DimBase setTopic(String topic) {
		this.topic = topic;
		return this;
	}
	public final Kafka2DimBase setGroupId(String groupId) {
		this.groupId = groupId;
		return this;
	}
	public final Kafka2DimBase setCacheTabel(String cacheTabel) {
		this.cacheTabel = cacheTabel;
		return this;
	}
	public final Kafka2DimBase setValidType(String validType) {
		this.validType = validType;
		return this;
	}
	public final Kafka2DimBase setArgs(String[] args) {
		this.args = args;
		return this;
	}
	public final Kafka2DimBase setCacheKuduMaster(String cacheKuduMaster) {
		this.cacheKuduMaster = cacheKuduMaster;
		return this;
	}

	protected final void transform() throws InterruptedException, KuduException{
		validateParam();
		
		final JdbcModel model = initTidbModel(appname);
		
		final Set<String> notNullColumns = new HashSet<String>();
		//初始化
		JavaInputDStream<String> stream = init(notNullColumns,model);
		
		final String _validType = validType;
		final String[] _args = args;
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final String _appname = appname;
		
		final String _cacheTabel = cacheTabel;
		final String _cacheKuduMaster = cacheKuduMaster;
		final String _keyColumn = keyColumn;
		final String _valueColumn = valueColumn;
		final String _ftenancyColumn = ftenancyColumn;
		//提取有用数据内容
        JavaDStream<Object> arrayDStream = stream.flatMap(tuple2 -> {
			try {
				JSONObject data = JSONObject.parseObject(tuple2);
				
				JSONArray result = parserKafkaData(data);
				
				return result.iterator();
			} catch (Exception e) {
				System.out.println("数组 转换异常");
				e.printStackTrace();
				JSONArray datas = new JSONArray();
				
				return datas.iterator();
			}
        });
        
        JavaDStream<Object> tableFilterDStream = arrayDStream.filter(new Function<Object, Boolean>() {
        	private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Object data) throws Exception {
				JSONObject jsonData = (JSONObject)data;
				boolean isValid = validate(jsonData,_validType,_args);
				
				if(!isValid) {
					updateOffset(jsonData,_appname,JdbcFactory.getJDBCUtil(model));
				}
				return isValid;
			}
        });
        
        JavaDStream<JSONObject> tranfDStream = tableFilterDStream.map(tuple -> {
        	JSONObject jsonData = (JSONObject)tuple;
        	if(jsonData == null)return jsonData;
        	JSONObject mappingData = mappingData(jsonData);
        	
        	setDefaultColumn(mappingData,sdf);
        	
        	JSONObject messages = new JSONObject();
        	messages.put("_MESSAGE_", mappingData.toJSONString());
        	messages.put("_PARTITION_", jsonData.getInteger("_PARTITION_"));
        	messages.put("_OFFSET_", jsonData.getLong("_OFFSET_"));
        	messages.put("_TOPIC_", jsonData.getString("_TOPIC_"));
    		
        	return messages;
        });
        
        //必填字段为空的数据过滤
        JavaDStream<JSONObject> filterDStream = tranfDStream.filter(new Function<JSONObject, Boolean>() {
        	private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(JSONObject data) throws Exception {
				JSONObject jsonData = (JSONObject)data;

				for(String key : notNullColumns){
					String value = jsonData.getJSONObject("_MESSAGE_").getString(key);
					if(value == null || "".equals(value)){
						System.out.println("数据合法性判断：key : " + key + " value : " + value);
						
						updateOffset(jsonData,_appname,JdbcFactory.getJDBCUtil(model));
						
						return false;
					}
				}
				return true;
			}
        });
        
        //存储数据
        filterDStream.foreachRDD(rdd -> {
        	rdd.foreachPartition(partition -> {
        		JdbcUtils util = JdbcFactory.getJDBCUtil(model);
        		while(partition.hasNext()){
					JSONObject parJson = partition.next();
					//更新数据
					listSaveHandler(parJson.getJSONObject("_MESSAGE_"),_cacheTabel,_cacheKuduMaster);
					//跟新redis
					saveRedis(parJson.getJSONObject("_MESSAGE_"),_keyColumn,_valueColumn,_ftenancyColumn,_cacheTabel);
					//更新offset
					updateOffset(parJson,_appname,util);
				}
        	});
        });
        
        context.start();
        context.awaitTermination();
	}
	
	private final void validateParam() {
		if(broker == null || "".equals(broker)) {
			throw new RuntimeException("[broker]不能为空");
		}
		if(topic == null || "".equals(topic)) {
			throw new RuntimeException("[topic]不能为空");
		}
		if(cacheTabel == null || "".equals(cacheTabel)) {
			throw new RuntimeException("[cacheTabel]不能为空");
		}
		if(cacheKuduMaster == null || "".equals(cacheKuduMaster)) {
			throw new RuntimeException("[cacheKuduMaster]不能为空");
		}
		if(keyColumn == null || "".equals(keyColumn)) {
			throw new RuntimeException("[keyColumn]不能为空");
		}
		if(valueColumn == null || "".equals(valueColumn)) {
			throw new RuntimeException("[valueColumn]不能为空");
		}
		if(appname == null || "".equals(appname)) {
			appname = cacheTabel.replace(".", "_");
		}
		if(groupId == null || "".equals(groupId)) {
			groupId = cacheTabel.replace(".", "_");
		}
	}
	
	private void updateOffset(JSONObject data,String _appname,JdbcUtils util) {
		String insertSql = "insert into bi_online_offset(k_appname,k_topic,k_partition,k_date,k_offset) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE k_offset=?";
		Object[] params = new Object[]{
				_appname,
				data.getString("_TOPIC_"),
				data.getInteger("_PARTITION_"),
			new SimpleDateFormat("yyyy-MM-dd HH").format(new Date()),
			data.getLong("_OFFSET_"),
			data.getLong("_OFFSET_")};
		util.excuteSql(insertSql,params);
	}
	
	private void listSaveHandler(JSONObject result,String tableName,String kuduMaster){
		Map<String,Object> values = new HashMap<String,Object>();
		for(String key : result.keySet()){
			values.put(key, result.get(key));
		}
		try {
			KuduUtil.getKuduInstance(kuduMaster).updateColumn(tableName,values);
		} catch (KuduException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void saveRedis(JSONObject jsonObject, String _keyColumn, String _valueColumn,String _ftenancyColumn ,String _targetTabel) {
		try {
			String key = jsonObject.getString(_keyColumn);
			String ftenancy = jsonObject.getString(_ftenancyColumn);
			if(key == null || "".equals(key)) {
				return;
			}
			RedisUtil.write(_targetTabel.hashCode() + "_" + ftenancy + "_" + key, 30 * 24 *60 * 60 * 1000, jsonObject.getString(_valueColumn));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 设置默认列
	 * @param mappingData
	 */
	private void setDefaultColumn(JSONObject mappingData,SimpleDateFormat sdf) {
		mappingData.put("etl_date", sdf.format(new Date()));
	}

	private JSONArray parserKafkaData(JSONObject datas) {
		String messages = datas.getString("_MESSAGE_");
		
		JSONArray dataArray = parserData(messages);
		
		for(int i = 0;i < dataArray.size();i++){
			JSONObject content = dataArray.getJSONObject(i);
			content.put("_PARTITION_", datas.getInteger("_PARTITION_"));
			content.put("_OFFSET_", datas.getLong("_OFFSET_"));
			content.put("_TOPIC_", datas.getString("_TOPIC_"));
		}
		return dataArray;
	}
	
	/**
	 * 初始化
	 * @param appname
	 * @param topic
	 * @param groupId
	 * @param args
	 * @return
	 */
	private JavaInputDStream<String> init(Set<String> notNullColumns, JdbcModel model) throws KuduException{
		//初始化 intStreamingContext
		intStreamingContext();
		//初始化kudu
		initKudu(notNullColumns);
		//初始化offset
		final List<Map<String, Object>> offsets = initOffsets(model);
		//初始化kafkastream
		return initJavaInputDStream(offsets);
	}
	
	/**
	 * 初始化kudu链接对象
	 * @param context
	 * @return 
	 * @return 
	 * @throws KuduException 
	 */
	private void initKudu(Set<String> notNullColumns) throws KuduException{
		KuduContext kuduContext = new KuduContext(cacheKuduMaster, context.ssc().sc());
		System.out.println("kudu 初始化成功！");
		 
		KuduTable kuduTable = kuduContext.syncClient().openTable(cacheTabel);
		List<ColumnSchema> columns = kuduTable.getSchema().getColumns();
		System.out.println("kudu table[" + cacheTabel + "] 初始化成功！");
		
        for (ColumnSchema schema : columns) {
        	if(!schema.isNullable()){
        		notNullColumns.add(schema.getName());
        	}
        }
        
        System.out.println("kudu table[" + cacheTabel + "] structType 初始化成功！");
	}
	
	/**
	 * 初始化JavaStreamingContext
	 * @param context
	 * @return 
	 */
	private void intStreamingContext(){
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
		sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "20"); 
		
        context = new JavaStreamingContext(sparkConf, Seconds.apply(6));
        
        System.out.println("JavaStreamingContext 初始化成功！");
	}
	
	/**
	 * 初始jdbc链接model
	 * @param dbKey
	 * @return
	 * @throws KuduException
	 */
	private JdbcModel initTidbModel(String dbKey){
		return new JdbcModel(
				dbKey,
				ResourcesUtil.getValue("conf", "tidb.driverClassName"),
				ResourcesUtil.getValue("conf", "tidb.url"),
				ResourcesUtil.getValue("conf", "tidb.username"),
				ResourcesUtil.getValue("conf", "tidb.password")
				);
	}
	
	/**
	 * 初始化intiKafka
	 * @param context
	 * @return 
	 */
	private JavaInputDStream<String> initJavaInputDStream(List<Map<String, Object>> offsets){
		// 首先要创建一份kafka参数map
        Map<String, String> kafkaParams = new HashMap<>();
       
//        String offsetReset = ResourcesUtil.getValue("conf", "collect.auto.offset.reset");
        String fetchMaxBytes = ResourcesUtil.getValue("conf", "collect.fetch.message.max.bytes");
        // 这里是不需要zookeeper节点,所以这里放broker.list
        kafkaParams.put("metadata.broker.list",broker);
//        kafkaParams.put("auto.offset.reset", offsetReset);
        kafkaParams.put("group.id", groupId );
        kafkaParams.put("fetch.message.max.bytes", fetchMaxBytes);
        Set<String> topicSet = new HashSet<String>();
        
        topicSet.add(topic);
        System.out.println("kafka 初始化成功！");
        Map<TopicAndPartition,Long> tpMap = null;
        if(offsets == null || offsets.size() == 0) {
        	List<String> topics = new ArrayList<>();
    	    topics.add(topic);
            tpMap = KafkaUtil.getInstance().getLastOffset(broker, topics, groupId);
        }else {
        	tpMap = new HashMap<>();
        	for(Map<String, Object> partition : offsets) {
        		tpMap.put(new TopicAndPartition(partition.get("k_topic").toString(),Integer.parseInt(partition.get("k_partition").toString())), Long.parseLong(partition.get("k_offset").toString()));
        	}
        }
        
        return KafkaUtils.createDirectStream(context, String.class, String.class, StringDecoder.class, StringDecoder.class,String.class, kafkaParams, tpMap, new Function<MessageAndMetadata<String,String>,String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(MessageAndMetadata<String, String> v1) throws Exception {
				JSONObject result = new JSONObject();
				result.put("_MESSAGE_", v1.message());
				result.put("_PARTITION_", v1.partition());
				result.put("_OFFSET_", v1.offset());
				result.put("_TOPIC_", v1.topic());
				return result.toJSONString();
			}
        	
        });
	}
	
	/**
	 * 初始化offset
	 * @param topic
	 * @param args
	 * @return
	 */
	private List<Map<String, Object>> initOffsets(JdbcModel model){
		String sql = null;
        Object[] initParams = null;
        if(args.length > 0) {
        	initParams = new Object[]{topic,args[0],appname};
        	sql = "select * from bi_online_offset where k_topic=? and k_date=? and k_appname=?";
        }else {
        	sql = "select o.k_topic,o.k_partition,o.k_offset \r\n" + 
        			"from bi_online_offset o\r\n" + 
        			"INNER JOIN (\r\n" + 
        			"	select max(k_date) as k_date,k_topic,k_partition,k_appname from bi_online_offset \r\n" + 
        			"	where k_topic=? \r\n" + 
        			"	GROUP BY k_topic,k_partition,k_appname\r\n" + 
        			") t on t.k_date = o.k_date and t.k_partition = o.k_partition and t.k_topic = o.k_topic and o.k_appname=t.k_appname\r\n" + 
        			"where o.k_topic=? and o.k_appname=?";
        	initParams = new Object[]{topic,topic,appname};
        }
        List<Map<String, Object>> offsets = JdbcFactory.getJDBCUtil(model).getResultSet(sql,initParams);
        return offsets;
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

	/**
	 * 做数据过滤。符合条件的返回true 不符合条件的返回false
	 * @param jsonData
	 * @param _validType
	 * @param _args 
	 * @return
	 */
	protected Boolean validate(JSONObject jsonData,String _validType, String[] _args) {
		if(_validType == null || "".equals(_validType)) {
			return true;
		}
		Pattern pattern = Pattern.compile(_validType + "(_)?[0-9]*");
		String tableName = jsonData.getString("tableName");
	    Matcher isNum = pattern.matcher(tableName);
	    if (!isNum.matches()) {
	    	return false;
	    }
		return true;
	}
	
	/**
	 * 字段etl映射匹配
	 * @param jsonData
	 * @param columns
	 * @return
	 */
	protected abstract JSONObject mappingData(JSONObject jsonData);
}
