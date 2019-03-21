package sparkKudu;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kudu.client.KuduException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.jdbc.JdbcFactory;
import com.whz.platform.sparkKudu.jdbc.JdbcUtils;
import com.whz.platform.sparkKudu.kudu.KuduUtil;
import com.whz.platform.sparkKudu.model.JdbcModel;
import com.whz.platform.sparkKudu.util.ResourcesUtil;

import kafka.serializer.StringDecoder;

public class Kafka2TidbUser_batch {
	private static JavaStreamingContext context;
	
	public static void main(String[] args) throws InterruptedException, IOException {
		final String appname = "event_b2b2c_user_tidb";
		final String groupId = "event_b2b2c_user_tidb2fd";
		final String kuduMaster = ResourcesUtil.getValue("conf", "kudu.master");
		
		final String tablename = "bi_user_tmp";
		
		final String topic = ResourcesUtil.getValue("conf", "collect.user.topic");
		final String validType = "t_user_buyer";

		//初始化JavaStreamingContext JavaSparkContext SparkSession
		intStreamingContext(appname);
        //获取kafka数据流
        JavaPairInputDStream<String, String> stream = intiKafka(context, topic,groupId);
        //初始化kudu对象
        JdbcModel jdbcModel = initTidbModel();
        
        final Set<String> columns = new HashSet<String>();
        
        setCalculateColumns(columns);
        
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
        
        //垃圾数据过滤
	    JavaDStream<Object> filterDStream = arrayDStream.filter(new Function<Object, Boolean>() {
	    	private static final long serialVersionUID = 1L;
	
			@Override
			public Boolean call(Object data) throws Exception {
				JSONObject jsonData = (JSONObject)data;
	
				return isValidData(jsonData,validType);
			}
	    });
        
        JavaDStream<JSONObject> tranfDStream = filterDStream.map(tuple -> {
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
        		if(partition.hasNext()){
					listSaveHandler(partition,tablename,util,columns);
				}
        	});
        });
        
        context.start();
        context.awaitTermination();
	}
	
	private static void setCalculateColumns(Set<String> columns) {
		columns.add("k_ftenancy_id");
		columns.add("k_fid");
		columns.add("fmid");
		columns.add("fmobile");
		columns.add("femail");
		columns.add("fphoto");
		columns.add("faccount_type");
		columns.add("flogin_account");
		columns.add("fusertype");
		columns.add("fproperty");
		columns.add("fdiffsrcregtime");
		columns.add("frating");
		columns.add("fbabyidlist");
		columns.add("frelationwithbaby");
		columns.add("frelationwithbaby_name");
		columns.add("fnickname");
		columns.add("ftruename");
		columns.add("fsex");
		columns.add("fsex_name");
		columns.add("fbirthday");
		columns.add("fregion");
		columns.add("fprovincename");
		columns.add("fcityname");
		columns.add("fdistrictname");
		columns.add("fcommunity");
		columns.add("faddress");
		columns.add("fpostcode");
		columns.add("faddtime");
		columns.add("flastupdatetime");
		columns.add("freferrer");
		columns.add("freferrer_name");
		columns.add("fmemberlevel");
		columns.add("fmembercardlist");
		columns.add("frecruiter");
		columns.add("frecruiter_name");
		columns.add("fmanager");
		columns.add("fmanager_name");
		columns.add("fcreator");
		columns.add("fcreator_name");
		columns.add("fcreatordepartment");
		columns.add("fcreatordepartment_name");
		columns.add("fregistersource");
		columns.add("fregistersource_name");
		columns.add("fmembersource");
		columns.add("fmembersource_name");
		columns.add("fdiffchannelactivetime");
		columns.add("fpromoteactive");
		columns.add("fpromoteactive_name");
		columns.add("fuserlablelist");
		columns.add("fuserlableremarks");
		columns.add("fmobilestatus");
		columns.add("fmemberproperty");
		columns.add("fuserpicturelablelist");
		columns.add("fpregnantplan");
		columns.add("fpaidmemberlevel");
		columns.add("fnotename");
		columns.add("fabcinviter");
		columns.add("fbirthday_next");
		columns.add("fstatus");
		columns.add("fproperty_str");
		columns.add("fbindweichat");
		
		
	}

	private static JSONObject mappingData(JSONObject jsonData,Set<String> columns,String kuduMaster) throws ParseException {
		JSONObject mappingData = new JSONObject();
		for(String columnName : columns){
			if("k_ftenancy_id".equals(columnName)){
				String value = jsonData.getString("FtenancyId");
				mappingData.put(columnName, value);
			}else if("k_fid".equals(columnName)){
				String value = jsonData.getString("Fuid");
				mappingData.put(columnName, value);
			}else if("fmid".equals(columnName)){
				try {
					Long value = jsonData.getLong("Fmid");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fmobile".equals(columnName)){
				String value = jsonData.getString("Fmobile");
				mappingData.put(columnName, value);
			}else if("femail".equals(columnName)){
				String value = jsonData.getString("Femail");
				mappingData.put(columnName, value);
			}else if("fphoto".equals(columnName)){
				String value = jsonData.getString("Fphoto");
				mappingData.put(columnName, value);
			}else if("faccount_type".equals(columnName)){
				try {
					int value = jsonData.getInteger("Faccount_type");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("flogin_account".equals(columnName)){
				String value = jsonData.getString("Flogin_account");
				mappingData.put(columnName, value);
			}else if("fusertype".equals(columnName)){
				try {
					int value = jsonData.getInteger("Fusertype");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fproperty".equals(columnName)){
				try {
					long value = jsonData.getLong("Fproperty");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fdiffsrcregtime".equals(columnName)){
				String value = jsonData.getString("FdiffSrcRegTime");
				mappingData.put(columnName, value);
			}else if("frating".equals(columnName)){
				try {
					long value = jsonData.getLong("Frating");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fbabyidlist".equals(columnName)){
				String value = jsonData.getString("FbabyIdList");
				mappingData.put(columnName, value);
			}else if("frelationwithbaby".equals(columnName)){
				try {
					int value = jsonData.getIntValue("FrelationWithBaby");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}
			else if("frelationwithbaby_name".equals(columnName)){
				try {
					int value = jsonData.getInteger("FrelationWithBaby");
					String frelationwithbaby_name = "";
					if(value == 0){
						frelationwithbaby_name = "未知";
					}else if(value == 1){
						frelationwithbaby_name = "父亲";
					}else if(value == 2){
						frelationwithbaby_name = "母亲";
					}else if(value == 3){
						frelationwithbaby_name = "爷爷";
					}else if(value == 4){
						frelationwithbaby_name = "奶奶";
					}else if(value == 5){
						frelationwithbaby_name = "外公";
					}else if(value == 6){
						frelationwithbaby_name = "外婆";
					}else if(value == 7){
						frelationwithbaby_name = "无关系";
					}else if(value == 8){
						frelationwithbaby_name = "其他";
					}
					mappingData.put(columnName, frelationwithbaby_name);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fnickname".equals(columnName)){
				String value = jsonData.getString("Fnickname");
				
				mappingData.put(columnName, value);
			}else if("ftruename".equals(columnName)){
				String value = jsonData.getString("Ftruename");
				
				mappingData.put(columnName, value);
			}else if("fsex".equals(columnName)){
				String value = jsonData.getString("Fsex");
				if("".equals(value)){
					mappingData.put(columnName, "0");
				}else{
					mappingData.put(columnName, value);
				}
			}else if("fsex_name".equals(columnName)){
				try {
					int value = jsonData.getInteger("Fsex");
					String fsex_name = "";
					if(value == 0){
						fsex_name = "未知";
					}else if(value == 1){
						fsex_name = "女";
					}else if(value == 2){
						fsex_name = "男";
					}
					mappingData.put(columnName, fsex_name);
				} catch (Exception e) {
					mappingData.put(columnName, "未知");
				}
			}else if("fbirthday".equals(columnName)){
				String value = jsonData.getString("Fbirthday");
				
				mappingData.put(columnName, value);
			}else if("fregion".equals(columnName)){
				String value = jsonData.getString("Fregion");
				
				mappingData.put(columnName, value);
				if(value == null || "".equals(value)){
					continue;
				}
				try {
					List<String> columnNames =new ArrayList<>();
					columnNames.add("fprovincename");
					columnNames.add("fcityname");
					columnNames.add("fdistrictname");
					
					Map<String,Object> params = new HashMap<String,Object>();
			        params.put("fprovincesysno", value.split("_")[0]);
			        params.put("fcitysysno", value.split("_")[1]);
			        params.put("fdistrictsysno", value.split("_")[2]);
					List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_pro_city_area_dim", params,columnNames);
					if(list.size() > 0){
						Object fprovincename = list.get(0).get("fprovincename");
						Object fcityname = list.get(0).get("fcityname");
						Object fdistrictname = list.get(0).get("fdistrictname");
						
						mappingData.put("fprovincename", fprovincename);
						mappingData.put("fcityname", fcityname);
						mappingData.put("fdistrictname", fdistrictname);
					}
				} catch (Exception e) {
					mappingData.put("fprovincename", "");
					mappingData.put("fcityname", "");
					mappingData.put("fdistrictname", "");
				}
			}else if("fcommunity".equals(columnName)){
				String value = jsonData.getString("Fcommunity");
				
				mappingData.put(columnName, value);
			}else if("faddress".equals(columnName)){
				String value = jsonData.getString("Faddress");
				
				mappingData.put(columnName, value);
			}else if("fpostcode".equals(columnName)){
				String value = jsonData.getString("Fpostcode");
				
				mappingData.put(columnName, value);
			}else if("faddtime".equals(columnName)){
				String value = jsonData.getString("Faddtime");
				
				mappingData.put(columnName, value);
			}else if("flastupdatetime".equals(columnName)){
				String value = jsonData.getString("Flastupdatetime");
				
				mappingData.put(columnName, value);
			}else if("freferrer".equals(columnName)){
				String value = jsonData.getString("Freferrer");
				
				mappingData.put(columnName, value);
			}else if("freferrer_name".equals(columnName)){
				try {
					List<String> columnNames =new ArrayList<>();
					columnNames.add("name");
					
					Map<String,Object> params = new HashMap<String,Object>();
			        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
			        params.put("code", jsonData.getString("Freferrer"));
					List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_name_code_dim", params,columnNames);
					if(list.size() > 0){
						Object name = list.get(0).get("name");
						mappingData.put(columnName, name);
					}else {
						mappingData.put(columnName, null);
					}
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fmemberlevel".equals(columnName)){
				try {
					int value = jsonData.getIntValue("FmemberLevel");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fmembercardlist".equals(columnName)){
				String value = jsonData.getString("FmembercardList");
				
				mappingData.put(columnName, value);
			}else if("frecruiter".equals(columnName)){
				String value = jsonData.getString("Frecruiter");
				
				mappingData.put(columnName, value);
			}else if("frecruiter_name".equals(columnName)){
				try {
					List<String> columnNames =new ArrayList<>();
					columnNames.add("name");
					
					Map<String,Object> params = new HashMap<String,Object>();
			        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
			        params.put("code", jsonData.getString("Frecruiter"));
					List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_name_code_dim", params,columnNames);
					if(list.size() > 0){
						Object name = list.get(0).get("name");
						mappingData.put(columnName, name);
					}else {
						mappingData.put(columnName, null);
					}
				} catch (Exception e) {
					mappingData.put(columnName, "");
				}
			}else if("fmanager".equals(columnName)){
				String value = jsonData.getString("Fmanager");
				
				mappingData.put(columnName, value);
			}else if("fmanager_name".equals(columnName)){
				try {
					List<String> columnNames =new ArrayList<>();
					columnNames.add("name");
					
					Map<String,Object> params = new HashMap<String,Object>();
			        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
			        params.put("code", jsonData.getString("Fmanager"));
					List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_name_code_dim", params,columnNames);
					if(list.size() > 0){
						Object name = list.get(0).get("name");
						mappingData.put(columnName, name);
					}else {
						mappingData.put(columnName, null);
					}
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fcreator".equals(columnName)){
				String value = jsonData.getString("Fcreator");
				
				mappingData.put(columnName, value);
			}else if("fcreator_name".equals(columnName)){
				try {
					List<String> columnNames =new ArrayList<>();
					columnNames.add("name");
					
					Map<String,Object> params = new HashMap<String,Object>();
			        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
			        params.put("code", jsonData.getString("Fcreator"));
					List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_name_code_dim", params,columnNames);
					if(list.size() > 0){
						Object name = list.get(0).get("name");
						mappingData.put(columnName, name);
					}else {
						mappingData.put(columnName, null);
					}
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fcreatordepartment".equals(columnName)){
				String value = jsonData.getString("FcreatorDepartment");
				
				mappingData.put(columnName, value);
			}
			else if("fcreatordepartment_name".equals(columnName)){
				try {
					List<String> columnNames =new ArrayList<>();
					columnNames.add("storename");
					
					Map<String,Object> params = new HashMap<String,Object>();
			        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
			        params.put("storecode", jsonData.getString("FcreatorDepartment"));
					List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_baby_store_store_dim", params,columnNames);
					if(list.size() > 0){
						Object storename = list.get(0).get("storename");
						mappingData.put(columnName, storename);
					}else {
						mappingData.put(columnName, null);
					}
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fregistersource".equals(columnName)){
				try {
					int value = jsonData.getIntValue("FregisterSource");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fregistersource_name".equals(columnName)){
				try {
					int value = jsonData.getIntValue("FregisterSource");
					String fregistersource_name = "";
					if(value == 1){
						fregistersource_name = "零售共场工具端";
					}else if(value == 12){
						fregistersource_name = "ERP导入";
					}else if(value == 2){
						fregistersource_name = "后台";
					}else if(value == 20){
						fregistersource_name = "小程序";
					}else if(value == 21){
						fregistersource_name = "云pos";
					}else if(value == 3){
						fregistersource_name = "宝宝店思迅同步";
					}else if(value == 5){
						fregistersource_name = "pos端";
					}else if(value == 6){
						fregistersource_name = "app";
					}else if(value == 7){
						fregistersource_name = "微商城";
					}else{
						fregistersource_name = "其他";
					}
					mappingData.put(columnName, fregistersource_name);
				} catch (Exception e) {
					mappingData.put(columnName, "其他");
				}
			}
			else if("fmembersource".equals(columnName)){
				try {
					int value = jsonData.getIntValue("FmemberSource");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fmembersource_name".equals(columnName)){
				try {
					List<String> columnNames =new ArrayList<>();
					columnNames.add("fmembersource_name");
					
					Map<String,Object> params = new HashMap<String,Object>();
			        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
			        params.put("fmembersource", jsonData.getString("FmemberSource"));
					List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_user_source_dim", params,columnNames);
					if(list.size() > 0){
						Object fmembersource_name = list.get(0).get("fmembersource_name");
						mappingData.put(columnName, fmembersource_name);
					}else {
						mappingData.put(columnName, null);
					}
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fdiffchannelactivetime".equals(columnName)){
				String value = jsonData.getString("FdiffChannelActiveTime");
				
				mappingData.put(columnName, value);
			}else if("fpromoteactive".equals(columnName)){
				String value = jsonData.getString("FpromoteActive");
				
				mappingData.put(columnName, value);
			}else if("fpromoteactive_name".equals(columnName)){
				try {
					List<String> columnNames =new ArrayList<>();
					columnNames.add("name");
					
					Map<String,Object> params = new HashMap<String,Object>();
			        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
			        params.put("code", jsonData.getString("FpromoteActive"));
					List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_name_code_dim", params,columnNames);
					if(list.size() > 0){
						Object name = list.get(0).get("name");
						mappingData.put(columnName, name);
					}else {
						mappingData.put(columnName, null);
					}
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}
			else if("fuserlablelist".equals(columnName)){
				String value = jsonData.getString("FuserLableList");
				
				mappingData.put(columnName, value);
			}else if("fuserlableremarks".equals(columnName)){
				String value = jsonData.getString("FuserLableRemarks");
				
				mappingData.put(columnName, value);
			}else if("fmobilestatus".equals(columnName)){
				try {
					int value = jsonData.getIntValue("FmobileStatus");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fmemberproperty".equals(columnName)){
				try {
					long value = jsonData.getLongValue("FmemberProperty");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fuserpicturelablelist".equals(columnName)){
				String value = jsonData.getString("FuserPictureLableList");
				
				mappingData.put(columnName, value);
			}else if("fpregnantplan".equals(columnName)){
				try {
					int value = jsonData.getIntValue("FpregnantPlan");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fpaidmemberlevel".equals(columnName)){
				String value = jsonData.getString("FpaidMemberLevel");
				
				mappingData.put(columnName, value);
			}else if("fnotename".equals(columnName)){
				String value = jsonData.getString("FnoteName");
				
				mappingData.put(columnName, value);
			}else if("fabcinviter".equals(columnName)){
				String value = jsonData.getString("FabcInviter");
				mappingData.put(columnName, value);
			}
			else if("fbirthday_next".equals(columnName)){
				String fbirthday = jsonData.getString("Fbirthday");
				if(fbirthday.length() >= 10){
					String value = jsonData.getString("Fbirthday").substring(5,10);
					String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
					String currentYear = currentDate.substring(0, 4);
					String currentMonDay = currentDate.substring(5, 10);
					String fbirthday_next = "";
					if(currentMonDay.compareTo(value) > 0){
						fbirthday_next = (Integer.parseInt(currentYear) + 1) + "-" + value;
					}else{
						fbirthday_next = currentYear + "-" + value;
					}
					mappingData.put(columnName, fbirthday_next);
				}else{
					mappingData.put(columnName, null);
				}
			}
			else if("fstatus".equals(columnName)){
				try {
					int value = jsonData.getIntValue("Fstatus");
					mappingData.put(columnName, value);
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}else if("fproperty_str".equals(columnName)){
				try {
					long fproperty = jsonData.getLongValue("Fproperty");
					mappingData.put(columnName, Long.toBinaryString(fproperty));
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}
			else if("fbindweichat".equals(columnName)){
				try {
					long fproperty = jsonData.getLongValue("Fproperty");
					String binary = Long.toBinaryString(fproperty);
					char c = binary.charAt(binary.length() - 4);
					mappingData.put(columnName,Integer.parseInt(c + "") );
				} catch (Exception e) {
					mappingData.put(columnName, null);
				}
			}
		}
		return mappingData;
	}
	
	protected static Boolean isValidData(JSONObject jsonData,String validType) {
		if(jsonData == null){
			System.out.println("数据为null直接返回");
			return false;
		}
		if(!validType.equalsIgnoreCase(jsonData.getString("tableName"))){
			return false;
		}
		return true;
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

	public static void listSaveHandler(Iterator<JSONObject> partition,String tableName,JdbcUtils util,Set<String> columns){
		PreparedStatement psmt = null;
		try {
			util.getConnection().setAutoCommit(false);
			String sql = getSql(columns,tableName);
			psmt = util.getPreparedStatement(sql);
			
			while(partition.hasNext()) {
				Map<String,Object> result = partition.next();
				
				int insertIndex = 0;
				int updateIndex = columns.size();
				Object[] params = new Object[columns.size() +columns.size()];
				
				for(String column : columns){
					params[insertIndex] = result.get(column);
					insertIndex++;
					
					params[updateIndex] = result.get(column);
					updateIndex++;
				}
				util.addBatch(psmt, params);
			}
			psmt.executeBatch();
			util.getConnection().commit();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(psmt != null) {
				try {
					psmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
   }
	
	private static String getSql(Set<String> columns,String tableName) {
		String columnSql = "";
		String valueSql = "";
		String updateSql = "";
		for(String column : columns){
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
			if("".equals(updateSql)){
				updateSql += column + "=?";
			}else{
				updateSql += "," + column + "=?";
			}
		}
		String insertSql = "insert into " + tableName +" (" + columnSql + ") VALUES(" + valueSql + ") ON DUPLICATE KEY UPDATE " + updateSql;
		
		return insertSql;
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
