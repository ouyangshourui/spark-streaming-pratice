package com.whz.streaming;

import java.io.Serializable;

import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.util.ResourcesUtil;
import com.whz.platform.sparkKudu.util.StringUtils;
import com.whz.streaming.base.Kafka2TidbBase;

public class Kafka2TidbScore extends Kafka2TidbBase implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) throws Exception {
		Kafka2TidbScore tidbUser = new Kafka2TidbScore();
		tidbUser.setAppname("event_b2b2c_userscore_tidb")
			.setGroupId("event_b2b2c_userscore_tidb")
			.setTargetTabel("bi_user")
			.setTopic(ResourcesUtil.getValue("conf", "collect.user.topic"))
			.setValidType("t_score_new")
			.setArgs(args)
			.setBroker(ResourcesUtil.getValue("conf", "collect.metadata.broker.list"));
		tidbUser.setTargetDb("bi");
        //执行etl清洗
        tidbUser.transform();
	}
	
	@Override
	protected Boolean validate(JSONObject jsonData,String validType, String[] _args) {
		if(!"2".equals(jsonData.getString("fAccountType")) && !"4".equals(jsonData.getString("fAccountType"))){
	    	return false;
	    }
		return super.validate( jsonData,validType,  _args);
	}

	@Override
	protected JSONObject mappingData(JSONObject jsonData) throws Exception{
		JSONObject mappingData = new JSONObject();
		mappingData.put("k_ftenancy_id", StringUtils.getVolidString(jsonData.getString("fTenancyId")));
		mappingData.put("k_fid", StringUtils.getVolidString(jsonData.getString("fUid")));
		int faccountType = jsonData.getIntValue("fAccountType");
		if(faccountType == 2){
			String value = StringUtils.getVolidString(jsonData.getString("fAccountPoints"));//fAccountPoints
			mappingData.put("faccountpoints", value);
		}
		if(faccountType == 4){
			String value = StringUtils.getVolidString(jsonData.getString("fAccountPoints"));
			mappingData.put("fmlevelvalue", value);
		}
		return mappingData;
	
	}

}
