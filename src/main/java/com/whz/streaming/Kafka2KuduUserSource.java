package com.whz.streaming;

import java.io.Serializable;

import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.jdbc.JdbcUtils;
import com.whz.platform.sparkKudu.util.ResourcesUtil;
import com.whz.platform.sparkKudu.util.StringUtils;
import com.whz.streaming.base.Kafka2KuduBase;

public class Kafka2KuduUserSource extends Kafka2KuduBase implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) throws Exception {
		
		Kafka2KuduUserSource kuduUser = new Kafka2KuduUserSource();
		kuduUser.setAppname("event_b2b2c_user_source_kudu")
			.setTopic(ResourcesUtil.getValue("conf", "collect.user.topic"))
			.setGroupId("event_b2b2c_user_source_kudu")
			.setArgs(args)
			.setBroker(ResourcesUtil.getValue("conf", "collect.metadata.broker.list"))
			.setKuduMaster(ResourcesUtil.getValue("conf", "kudu.master"))
			.setTargetTabel("event.event_user_source_dim")
			.setValidType("t_user_source");
		
		kuduUser.transform();
	}

	@Override
	protected JSONObject mappingData(JSONObject jsonData) {
		System.out.println(jsonData);
		JSONObject mappingData = new JSONObject();
		mappingData.put("k_ftenancy_id", StringUtils.getVolidString(jsonData.getString("fTenancyId")));
		mappingData.put("fmembersource", StringUtils.getVolidString(jsonData.getString("fMemberSource")));
		mappingData.put("fmembersource_name", StringUtils.getVolidString(jsonData.getString("fMemberSourceName")));
		mappingData.put("fsourcestatus", StringUtils.getVolidString(jsonData.getString("fSourceStatus")));
				
		return mappingData;
	}
	
	@Override
	protected void kuduSaveHandler(JSONObject result, String tableName, String kuduMaster, JdbcUtils util) {
//		super.kuduSaveHandler(result, tableName, kuduMaster, util);
	}
}
