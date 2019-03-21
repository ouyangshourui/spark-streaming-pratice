package com.whz.streaming;

import java.io.Serializable;

import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.util.ResourcesUtil;
import com.whz.platform.sparkKudu.util.StringUtils;
import com.whz.streaming.base.Kafka2KuduBase;


public class Kafka2KuduScore extends Kafka2KuduBase implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) throws Exception {
		
		Kafka2KuduScore kuduScoreUser = new Kafka2KuduScore();
		kuduScoreUser.setAppname("event_b2b2c_userscore_kudu")
			.setTopic(ResourcesUtil.getValue("conf", "collect.user.topic"))
			.setGroupId("event_b2b2c_userscore_kudu")
			.setArgs(args)
			.setBroker(ResourcesUtil.getValue("conf", "collect.metadata.broker.list"))
			.setKuduMaster(ResourcesUtil.getValue("conf", "kudu.master"))
			.setTargetTabel("event.event_user_user")
			.setValidType("t_score_new");
		
		kuduScoreUser.transform();
	}
	
	@Override
	protected Boolean validate( JSONObject jsonData,String validType, String[] _args) {
		if(!"2".equals(jsonData.getString("fAccountType")) && !"4".equals(jsonData.getString("fAccountType"))){
	    	return false;
	    }
		return super.validate( jsonData,validType , _args);
	}

	@Override
	protected JSONObject mappingData(JSONObject jsonData) {

		JSONObject mappingData = new JSONObject();
		mappingData.put("k_ftenancy_id", StringUtils.getVolidString(jsonData.getString("fTenancyId")));
		mappingData.put("k_fid", StringUtils.getVolidString(jsonData.getString("fUid")));
		int faccountType = jsonData.getIntValue("fAccountType");
		if(faccountType == 2){
			mappingData.put("faccountpoints", StringUtils.getVolidString(jsonData.getString("fAccountPoints")));
		}
		if(faccountType == 4){
			mappingData.put("fmlevelvalue", StringUtils.getVolidString(jsonData.getString("fAccountPoints")));
		}
		return mappingData;
	
	}
}
