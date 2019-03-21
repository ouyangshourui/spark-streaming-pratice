package com.whz.streaming;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.util.ResourcesUtil;
import com.whz.platform.sparkKudu.util.StringUtils;
import com.whz.streaming.base.Kafka2KuduBase;

public class Kafka2KuduBaby extends Kafka2KuduBase implements Serializable{

	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) throws Exception {
		
		Kafka2KuduBaby kuduBaby = new Kafka2KuduBaby();
		kuduBaby.setTopic(ResourcesUtil.getValue("conf", "collect.user.topic"))
				.setArgs(args)
				.setBroker(ResourcesUtil.getValue("conf", "collect.metadata.broker.list"))
				.setKuduMaster(ResourcesUtil.getValue("conf", "kudu.master"))
				.setTargetTabel("event.event_user_baby_baby")
				.setValidType("t_user_baby");
		
		kuduBaby.transform();
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
}
