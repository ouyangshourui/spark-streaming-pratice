package com.whz.streaming;

import java.io.Serializable;

import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.jdbc.JdbcUtils;
import com.whz.platform.sparkKudu.util.ResourcesUtil;
import com.whz.platform.sparkKudu.util.StringUtils;
import com.whz.streaming.base.Kafka2KuduBase;

public class Kafka2KuduUserLevel extends Kafka2KuduBase implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) throws Exception {
		
		Kafka2KuduUserLevel kuduUser = new Kafka2KuduUserLevel();
		kuduUser.setAppname("event_b2b2c_userlevel_kudu")
			.setTopic(ResourcesUtil.getValue("conf", "collect.user.topic"))
			.setGroupId("event_b2b2c_userlevel_kudu")
			.setArgs(args)
			.setBroker(ResourcesUtil.getValue("conf", "collect.metadata.broker.list"))
			.setKuduMaster(ResourcesUtil.getValue("conf", "kudu.master"))
			.setTargetTabel("event.event_tenancy_userlevel_dim")
			.setValidType("t_level_config");
		
		kuduUser.transform();
	}

	@Override
	protected JSONObject mappingData(JSONObject jsonData) {
		System.out.println(jsonData);
		JSONObject mappingData = new JSONObject();
		mappingData.put("k_ftenancy_id", StringUtils.getVolidString(jsonData.getString("fTenancyId")));
		mappingData.put("flevelid", StringUtils.getVolidString(jsonData.getString("fLevelId")));
		mappingData.put("flevelname", StringUtils.getVolidString(jsonData.getString("fLevelName")));
		mappingData.put("flevelgrowthfloor", StringUtils.getVolidString(jsonData.getString("fLevelGrowthFloor")));
		
				
		return mappingData;
	}

	@Override
	protected void kuduSaveHandler(JSONObject result, String tableName, String kuduMaster, JdbcUtils util) {
//		super.kuduSaveHandler(result, tableName, kuduMaster, util);
	}
	
	
}
