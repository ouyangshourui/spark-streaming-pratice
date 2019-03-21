package com.whz.streaming;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.hbase.HBaseUtil;
import com.whz.platform.sparkKudu.jdbc.JdbcFactory;
import com.whz.platform.sparkKudu.model.JdbcModel;
import com.whz.platform.sparkKudu.util.ResourcesUtil;
import com.whz.platform.sparkKudu.util.StringUtils;
import com.whz.streaming.base.Kafka2KuduBase;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Kafka2OmsABTable extends Kafka2KuduBase implements Serializable{

	private static final long serialVersionUID = 1L;
	private   String aTable;
	private   String bTable;
	private   String anorelation;
	public  static  int printNum;
	public  static  final  String[] columnFamilys={"info"};


	public String getAnorelation() {
		return anorelation;
	}

	public void setAnorelation(String anorelation) {
		this.anorelation = anorelation;
	}


	protected Boolean validate(JSONObject jsonData, String[] _args) {
		return super.validate(jsonData, aTable, _args) || super.validate(jsonData, bTable, _args) ;
	}
	
	public static void main(String[] args) throws Exception {

		if(null!=args[0])
		  printNum=Integer.valueOf(args[0]);
		
		Kafka2OmsABTable kuduDealTradeTable = new Kafka2OmsABTable();
		kuduDealTradeTable.setaTable("t_deal_active");
		kuduDealTradeTable.setbTable("t_trade");
		kuduDealTradeTable.setAnorelation("t_deal_norelation_trade");
		kuduDealTradeTable.setTopic(ResourcesUtil.getValue("conf", "collect.oms.topic"))
				.setArgs(args)
				.setAppname("Kafka2OmsABTable_test1")
				.setGroupId("Kafka2OmsABTable_test1")
				.setBroker(ResourcesUtil.getValue("conf", "collect.metadata.broker.list"))
				.setKuduMaster(ResourcesUtil.getValue("conf", "kudu.master"))
				.setTargetTabel("event.event_tenancy_user_sku_order_kudu_de");

		//全量写入A表

		if(null!=args[1]) {
			if(Boolean.getBoolean(args[1])) {
				HBaseUtil.getInstance().dropTable(kuduDealTradeTable.getaTable());
				HBaseUtil.getInstance().dropTable(kuduDealTradeTable.getbTable());
				HBaseUtil.getInstance().dropTable(kuduDealTradeTable.getAnorelation());
			}
		}

        HBaseUtil.getInstance().createTable(kuduDealTradeTable.getaTable(),columnFamilys);
		//全量写入B表
		HBaseUtil.getInstance().createTable(kuduDealTradeTable.getbTable(),columnFamilys);
		//A表位关联上的B记录的表
		HBaseUtil.getInstance().createTable(kuduDealTradeTable.getAnorelation(),columnFamilys);

		kuduDealTradeTable.transform();
	}

	@Override
	protected void transform() throws Exception{

		final JdbcModel model = initTidbModel(appname);

		validateParam();

		//初始化
		JavaInputDStream<String> stream = initStream(model);
		//final Set<String> notNullColumns = initKudu();

		final String _validType = validType;
		final String[] _args = args;
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final String _appname = appname;
		final String _targetTabel = targetTabel;
		final String _kuduMaster = kuduMaster;

		//1、提取有用数据内容
		JavaDStream<Object> arrayDStream = stream.flatMap(tuple2 -> {
			try {
				JSONObject data = JSONObject.parseObject(tuple2);
				System.out.println(data);

				JSONArray result = parserKafkaData(data);

				return result.iterator();
			} catch (Exception e) {
				System.out.println("数组 转换异常");
				e.printStackTrace();
				JSONArray datas = new JSONArray();

				return datas.iterator();
			}
		});


       if(printNum==1)
		arrayDStream.print(1);

		//2、提取有用数据内容

		JavaDStream<Object> tableFilterDStream = arrayDStream.filter(new Function<Object, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Object data) throws Exception {
				JSONObject jsonData = (JSONObject)data;
				boolean isValid = validate(jsonData,_args);

				if(!isValid) {
					updateOffset(jsonData,_appname, JdbcFactory.getJDBCUtil(model));
				}
				return isValid;
			}
		});

		if(printNum==2)
		tableFilterDStream.print(1);


		//3、将AB表写入到HBase



		JavaDStream<JSONObject> LoadABTable2HBaseDStream = tableFilterDStream.repartition(1).transform(rdd->{
			//1.Get connect  from pool
			//2.write a,b table  to hbase

			JavaPairRDD<String, JSONObject> paiRDD=rdd.mapToPair(x->{
				JSONObject jsonData = (JSONObject)x;
				String opt_time=jsonData.getString("optTime");
				return new Tuple2(opt_time,jsonData);
			});
			JavaPairRDD<String, JSONObject> sortRDD=paiRDD.sortByKey();

			sortRDD.foreachPartition(p->{
				HBaseUtil hbaseUtil = HBaseUtil.getInstance();
				HTable ahTable= hbaseUtil.getTableHTable(aTable);
				HTable bhTable= hbaseUtil.getTableHTable(bTable);

				while(p.hasNext()){
					JSONObject jsonData = p.next()._2;
					String tablename =jsonData.getString("tableName");
					if(aTable.equals(tablename)){
                      String a_table_key=jsonData.getString("Fdeal_id")+'_'+jsonData.getString("Fbdeal_id");
                      for(String key:jsonData.keySet()){
						  hbaseUtil.insertData(ahTable,a_table_key,columnFamilys[0],
									key,jsonData.getString(key));
						}
						System.out.println("write a table to hbase");
					}else{
						String b_table_key=jsonData.getString("Ftrade_id");

						for(String key:jsonData.keySet()){
							hbaseUtil.insertData(bhTable,b_table_key,columnFamilys[0],
									key,jsonData.getString(key));
						}
						System.out.println("write b table to hbase");

					}
				}
						hbaseUtil.closeTableHTable(ahTable);
						hbaseUtil.closeTableHTable(bhTable);
					}
			);
			return sortRDD.map(r->r._2);
		});
		if(printNum==3)
			LoadABTable2HBaseDStream.print(1);

        //
		/**
		 * 3、关联AB表
		 */

		LoadABTable2HBaseDStream.transform(rdd->{
			//1.Get connect  from pool
			//2.write a,b table  to hbase
			rdd.foreachPartition(p->{
				while(p.hasNext()){
					JSONObject jsonData = p.next();
					if(aTable.equals(jsonData.getString("tableName"))){
						// a表如果是insert，直接关联;如果是update，需要判断,四个是时间是否有空,没有的就写入

						System.out.println("write a table to hbase");
					}else{
						// b表如果是insert，直接放弃。如果是update，去和a表关联，关联不上直接放弃


						System.out.println("write b table to hbase");

					}
				}
			});
			return rdd;
		});
		context.start();
		context.awaitTermination();
	}

	protected Boolean WriteTable2HBase(String tablename, Array keyArray,String connection){


		return true;
	}
	@Override
	protected JSONObject mappingData(JSONObject jsonData) {
		JSONObject mappingData = new JSONObject();
		mappingData.put("k_ftenancy_id", StringUtils.getVolidString(jsonData.getString("FtenancyId")));
		mappingData.put("k_fid", StringUtils.getVolidString(jsonData.getString("Fmid")));
		mappingData.put("k_fbid", StringUtils.getVolidString(jsonData.getString("Fbid")));
		mappingData.put("ftruename", StringUtils.getVolidString(jsonData.getString("Ftruename")));
		mappingData.put("fnickname", StringUtils.getVolidString(jsonData.getString("Fnickname")));
		mappingData.put("flastupdatetime", StringUtils.getVolidString(jsonData.getString("Flastupdatetime")));
		mappingData.put("fphoto", StringUtils.getVolidString(jsonData.getString("Fphoto")));
		mappingData.put("fbirthday", StringUtils.getVolidString(jsonData.getString("Fbirthday")));
		mappingData.put("faddtime", StringUtils.getVolidString(jsonData.getString("Faddtime")));
		mappingData.put("fsex", StringUtils.getVolidString(jsonData.getString("Fsex")));
		mappingData.put("fstatus", StringUtils.getVolidString(jsonData.getString("Fstatus")));
		mappingData.put("fproperty", StringUtils.getVolidString(jsonData.getString("Fproperty")));
		mappingData.put("fexp_date_baby", StringUtils.getVolidString(jsonData.getString("Fbirthday")));
		if(StringUtils.getVolidString(jsonData.getString("Fbirthday")).length() >= 10){
			String value = StringUtils.getVolidString(jsonData.getString("Fbirthday")).substring(5,10);
			String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			String currentYear = currentDate.substring(0, 4);
			String currentMonDay = currentDate.substring(5, 10);
			String fbirthday_next = "";
			if(currentMonDay.compareTo(value) > 0){
				fbirthday_next = (Integer.parseInt(currentYear) + 1) + "-" + StringUtils.getVolidString(jsonData.getString("Fbirthday")).substring(5,10);
			}else{
				fbirthday_next = currentYear + "-" + StringUtils.getVolidString(jsonData.getString("Fbirthday")).substring(5,10);
			}
			mappingData.put("fnext_birthday", fbirthday_next);
		}
		String fbirthday = StringUtils.getVolidString(jsonData.getString("Fbirthday"));
		if(fbirthday.length() >= 10){
			int birthYear = Integer.parseInt(StringUtils.getVolidString(jsonData.getString("Fbirthday")).substring(0,4));
			int birthMonth = Integer.parseInt(StringUtils.getVolidString(jsonData.getString("Fbirthday")).substring(5,7));
			String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			int currentYear = Integer.parseInt(currentDate.substring(0, 4));
			int currentMonDay = Integer.parseInt(currentDate.substring(5, 7));
			int fbaby_age = (currentYear - birthYear )*12 + (currentMonDay - birthMonth);
			mappingData.put("fbaby_age", fbaby_age);
			
			String fbabyagestage = "";
			if (fbaby_age <-10) {
				fbabyagestage = "0";
			}else if (fbaby_age >=-10 && fbaby_age <= -7) {
				fbabyagestage = "1";
			}else if (fbaby_age >=-6 && fbaby_age <= -4) {
				fbabyagestage = "2";
			}else if (fbaby_age >=-3 && fbaby_age <= -1) {
				fbabyagestage = "3";
			}else if (fbaby_age >=0 && fbaby_age <= 3) {
				fbabyagestage = "4";
			}else if (fbaby_age >=4 && fbaby_age <= 6) {
				fbabyagestage = "5";
			}else if (fbaby_age >=7 && fbaby_age <= 12) {
				fbabyagestage = "6";
			}else if (fbaby_age >=13 && fbaby_age <= 24) {
				fbabyagestage = "7";
			}else if (fbaby_age >=25 && fbaby_age <= 36) {
				fbabyagestage = "8";
			}else if (fbaby_age >=37 && fbaby_age <= 72) {
				fbabyagestage = "9";
			}else if (fbaby_age >=73 && fbaby_age <= 168) {
				fbabyagestage = "10";
			}else {
				fbabyagestage = "11";
			}
			mappingData.put("fbabyagestage", fbabyagestage);
		}
		return mappingData;
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
		if(kuduMaster == null || "".equals(kuduMaster)) {
			throw new RuntimeException("[kuduMaster]不能为空");
		}
		if(appname == null || "".equals(appname)) {
			appname = targetTabel.replace(".", "_");
		}
		if(groupId == null || "".equals(groupId)) {
			groupId = targetTabel.replace(".", "_");
		}
	}
}
