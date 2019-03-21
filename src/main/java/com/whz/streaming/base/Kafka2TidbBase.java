package com.whz.streaming.base;

import java.text.SimpleDateFormat;

import com.whz.platform.sparkKudu.jdbc.JdbcUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.jdbc.JdbcFactory;
import com.whz.platform.sparkKudu.model.JdbcModel;

public abstract class Kafka2TidbBase extends KafkaStreamBase{
	
	private String targetDb;
	
	public final Kafka2TidbBase setTargetDb(String targetDb) {
		this.targetDb = targetDb;
		return this;
	}

	@Override
	protected void transform() throws Exception{
		validateParam();
		//初始化jdbc model
		final JdbcModel model = initTidbModel(appname);
		//初始化
		JavaInputDStream<String> stream = initStream(model);
		String dbTabel = "";
		if (targetTabel.indexOf(".") > -1) {
			dbTabel = targetTabel;
		}else {
			dbTabel = targetDb + "." + targetTabel;
		}
		final String _targetTabel = dbTabel;
		final String _validType = validType;
		final String[] _args = args;
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final String _appname = appname;
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
				boolean isValid = validate(jsonData,_validType ,_args);
				
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
        
        //存储数据
        tranfDStream.foreachRDD(rdd -> {
        	rdd.foreachPartition(partition -> {
        		JdbcUtils util = JdbcFactory.getJDBCUtil(model);
        		while(partition.hasNext()){
					JSONObject parJson = partition.next();
					//更新数据
					tidbSaveHandler(parJson.getJSONObject("_MESSAGE_"),util,_targetTabel);
					
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
		if(targetTabel == null || "".equals(targetTabel)) {
			throw new RuntimeException("[targetTabel]不能为空");
		}
		if(appname == null || "".equals(appname)) {
			appname = targetTabel.replace(".", "_");
		}
		if(groupId == null || "".equals(groupId)) {
			groupId = targetTabel.replace(".", "_");
		}
	}
}
