package com.whz.streaming;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.kudu.KuduUtil;
import com.whz.platform.sparkKudu.util.ResourcesUtil;
import com.whz.streaming.base.Kafka2TidbBase;

public class Kafka2TidbUser extends Kafka2TidbBase implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) throws Exception {
		 
		Kafka2TidbUser tidbUser = new Kafka2TidbUser();
		tidbUser.setAppname("event_b2b2c_user_tidb")
			.setGroupId("event_b2b2c_user_tidb")
			.setTargetTabel("bi_user")
			.setTopic(ResourcesUtil.getValue("conf", "collect.user.topic"))
			.setValidType("t_user_buyer")
			.setArgs(args)
			.setBroker(ResourcesUtil.getValue("conf", "collect.metadata.broker.list"));
		tidbUser.setTargetDb("bi");
        //执行etl清洗
        tidbUser.transform();
	}
	
	@Override
	protected JSONObject mappingData(JSONObject jsonData) throws Exception {
		String kuduMaster = ResourcesUtil.getValue("conf", "kudu.master");
		JSONObject mappingData = new JSONObject();
		mappingData.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
		mappingData.put("k_fid", jsonData.getString("Fuid"));
		try {
			mappingData.put("fmid", jsonData.getLong("Fmid"));
		} catch (Exception e) {
		}
		mappingData.put("fmobile", jsonData.getString("Fmobile"));
		mappingData.put("femail", jsonData.getString("Femail"));
		mappingData.put("fphoto", jsonData.getString("Fphoto"));
		try {
			mappingData.put("faccount_type", jsonData.getInteger("Faccount_type"));
		} catch (Exception e) {
		}
		mappingData.put("flogin_account", jsonData.getString("Flogin_account"));
		try {
			mappingData.put("fusertype", jsonData.getInteger("Fusertype"));
		} catch (Exception e) {
		}
		try {
			mappingData.put("fproperty", jsonData.getLong("Fproperty"));
		} catch (Exception e) {
		}
		mappingData.put("fdiffsrcregtime", jsonData.getString("FdiffSrcRegTime"));
		try {
			mappingData.put("frating", jsonData.getLong("Frating"));
		} catch (Exception e) {
		}
		mappingData.put("fbabyidlist", jsonData.getString("FbabyIdList"));
		try {
			mappingData.put("frelationwithbaby", jsonData.getIntValue("FrelationWithBaby"));
		} catch (Exception e) {
		}
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
			mappingData.put("frelationwithbaby_name", frelationwithbaby_name);
		} catch (Exception e) {
		}
		mappingData.put("fnickname", jsonData.getString("Fnickname"));
		mappingData.put("ftruename", jsonData.getString("Ftruename"));
		String sex = jsonData.getString("Fsex");
		if("".equals(sex)){
			mappingData.put("fsex", "0");
		}else{
			mappingData.put("fsex", sex);
		}
		try {
			int sexname = jsonData.getInteger("Fsex");
			String fsex_name = "";
			if(sexname == 0){
				fsex_name = "未知";
			}else if(sexname == 1){
				fsex_name = "女";
			}else if(sexname == 2){
				fsex_name = "男";
			}
			mappingData.put("fsex_name", fsex_name);
		} catch (Exception e) {
		}
		mappingData.put("fbirthday", jsonData.getString("Fbirthday"));
		mappingData.put("fregion", jsonData.getString("Fregion"));
		if(jsonData.getString("Fregion") != null && !"".equals(jsonData.getString("Fregion"))){
			try {
				List<String> columnNames =new ArrayList<>();
				columnNames.add("fprovincename");
				columnNames.add("fcityname");
				columnNames.add("fdistrictname");
				
				Map<String,Object> params = new HashMap<String,Object>();
		        params.put("fprovincesysno", jsonData.getString("Fregion").split("_")[0]);
		        params.put("fcitysysno", jsonData.getString("Fregion").split("_")[1]);
		        params.put("fdistrictsysno", jsonData.getString("Fregion").split("_")[2]);
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
			}
		}
		
		mappingData.put("fcommunity", jsonData.getString("Fcommunity"));
		mappingData.put("faddress", jsonData.getString("Faddress"));
		mappingData.put("fpostcode", jsonData.getString("Fpostcode"));
		try {
			String Faddtime = jsonData.getString("Faddtime");
			String fdiffSrcRegTime = jsonData.getString("FdiffSrcRegTime");
			if(fdiffSrcRegTime == null || "".equals(fdiffSrcRegTime)) {
				mappingData.put("faddtime", Faddtime);
			}else {
				String fregisterSource = jsonData.getString("FregisterSource");
				String[] regs = fdiffSrcRegTime.split(";");
				for(String str : regs) {
					if(fregisterSource.equals(str.split(":")[0])) {
						Faddtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(Long.parseLong(str.split(":")[1]) * 1000));
						mappingData.put("faddtime", Faddtime);
						break;
					}
				}
			}
		} catch (NumberFormatException e) {
		}
		mappingData.put("flastupdatetime", jsonData.getString("Flastupdatetime"));
		mappingData.put("freferrer", jsonData.getString("Freferrer"));
		if(jsonData.getString("Freferrer") != null && !"".equals(jsonData.getString("Freferrer"))){
			try {
				List<String> columnNames =new ArrayList<>();
				columnNames.add("name");
				
				Map<String,Object> params = new HashMap<String,Object>();
		        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
		        params.put("code", jsonData.getString("Freferrer"));
				List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_name_code_dim", params,columnNames);
				if(list.size() > 0){
					Object name = list.get(0).get("name");
					mappingData.put("freferrer_name", name);
				}
			} catch (Exception e) {
			}
		}
		try {
			mappingData.put("fmemberlevel", jsonData.getIntValue("FmemberLevel"));
		} catch (Exception e) {
		}
		mappingData.put("fmembercardlist", jsonData.getString("FmembercardList"));
		mappingData.put("frecruiter", jsonData.getString("Frecruiter"));
		if(jsonData.getString("Frecruiter") != null && !"".equals(jsonData.getString("Frecruiter"))){
			try {
				List<String> columnNames1 =new ArrayList<>();
				columnNames1.add("name");
				
				Map<String,Object> params1 = new HashMap<String,Object>();
				params1.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
				params1.put("code", jsonData.getString("Frecruiter"));
				List<Map<String, Object>> list1 = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_name_code_dim", params1,columnNames1);
				if(list1.size() > 0){
					Object name = list1.get(0).get("name");
					mappingData.put("frecruiter_name", name);
				}
			} catch (Exception e) {
			}
		}
		mappingData.put("fmanager", jsonData.getString("Fmanager"));
		if(jsonData.getString("Fmanager") != null && !"".equals(jsonData.getString("Fmanager"))){
			try {
				List<String> columnNames =new ArrayList<>();
				columnNames.add("name");
				
				Map<String,Object> params = new HashMap<String,Object>();
		        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
		        params.put("code", jsonData.getString("Fmanager"));
				List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_name_code_dim", params,columnNames);
				if(list.size() > 0){
					Object name = list.get(0).get("name");
					mappingData.put("fmanager_name", name);
				}
			} catch (Exception e) {
			}
		}
		
		mappingData.put("fcreator", jsonData.getString("Fcreator"));
		if(jsonData.getString("Fcreator") != null && !"".equals(jsonData.getString("Fcreator"))){
			try {
				List<String> columnNames =new ArrayList<>();
				columnNames.add("name");
				
				Map<String,Object> params = new HashMap<String,Object>();
		        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
		        params.put("code", jsonData.getString("Fcreator"));
				List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_name_code_dim", params,columnNames);
				if(list.size() > 0){
					Object name = list.get(0).get("name");
					mappingData.put("fcreator_name", name);
				}
			} catch (Exception e) {
			}
		}
		
		mappingData.put("fcreatordepartment", jsonData.getString("FcreatorDepartment"));
		if(jsonData.getString("FcreatorDepartment") != null && !"".equals(jsonData.getString("FcreatorDepartment"))){
			try {
				List<String> columnNames =new ArrayList<>();
				columnNames.add("storename");
				
				Map<String,Object> params = new HashMap<String,Object>();
		        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
		        params.put("storecode", jsonData.getString("FcreatorDepartment"));
				List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_baby_store_store_dim", params,columnNames);
				if(list.size() > 0){
					Object storename = list.get(0).get("storename");
					mappingData.put("fcreatordepartment_name", storename);
				}
			} catch (Exception e) {
			}
		}
		
		try {
			mappingData.put("fregistersource", jsonData.getIntValue("FregisterSource"));
		} catch (Exception e) {
		}
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
			mappingData.put("fregistersource_name", fregistersource_name);
		} catch (Exception e) {
		}
		try {
			int value = jsonData.getIntValue("FmemberSource");
			mappingData.put("fmembersource", value);
		} catch (Exception e) {
		}
		if(jsonData.getString("FmemberSource") != null && !"".equals(jsonData.getString("FmemberSource"))){
			try {
				List<String> columnNames =new ArrayList<>();
				columnNames.add("fmembersource_name");
				
				Map<String,Object> params = new HashMap<String,Object>();
		        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
		        params.put("fmembersource", jsonData.getString("FmemberSource"));
				List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_user_source_dim", params,columnNames);
				if(list.size() > 0){
					Object fmembersource_name = list.get(0).get("fmembersource_name");
					mappingData.put("fmembersource_name", fmembersource_name);
				}
			} catch (Exception e) {
			}
		}
		
		mappingData.put("fdiffchannelactivetime", jsonData.getString("FdiffChannelActiveTime"));
		mappingData.put("fpromoteactive", jsonData.getString("FpromoteActive"));
		if(jsonData.getString("FpromoteActive") != null && !"".equals(jsonData.getString("FpromoteActive"))){
			try {
				List<String> columnNames =new ArrayList<>();
				columnNames.add("name");
				
				Map<String,Object> params = new HashMap<String,Object>();
		        params.put("k_ftenancy_id", jsonData.getString("FtenancyId"));
		        params.put("code", jsonData.getString("FpromoteActive"));
				List<Map<String, Object>> list = KuduUtil.getKuduInstance(kuduMaster).scan("event.event_name_code_dim", params,columnNames);
				if(list.size() > 0){
					Object name = list.get(0).get("name");
					mappingData.put("fpromoteactive_name", name);
				}
			} catch (Exception e) {
			}
		}
		
		mappingData.put("fuserlablelist", jsonData.getString("FuserLableList"));
		mappingData.put("fuserlableremarks", jsonData.getString("FuserLableRemarks"));
		try {
			int value = jsonData.getIntValue("FmobileStatus");
			mappingData.put("fmobilestatus", value);
		} catch (Exception e) {
		}
		try {
			long value = jsonData.getLongValue("FmemberProperty");
			mappingData.put("fmemberproperty", value);
		} catch (Exception e) {
		}
		mappingData.put("fuserpicturelablelist", jsonData.getString("FuserPictureLableList"));
		try {
			int value = jsonData.getIntValue("FpregnantPlan");
			mappingData.put("fpregnantplan", value);
		} catch (Exception e) {
		}
		mappingData.put("fpaidmemberlevel", jsonData.getString("FpaidMemberLevel"));
		mappingData.put("fnotename", jsonData.getString("FnoteName"));
		mappingData.put("fabcinviter", jsonData.getString("FabcInviter"));
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
			mappingData.put("fbirthday_next", fbirthday_next);
		}else{
			mappingData.put("fbirthday_next", "");
		}
		try {
			int value = jsonData.getIntValue("Fstatus");
			mappingData.put("fstatus", value);
		} catch (Exception e) {
		}
		try {
			long fproperty = jsonData.getLongValue("Fproperty");
			mappingData.put("fproperty_str", Long.toBinaryString(fproperty));
		} catch (Exception e) {
			mappingData.put("fproperty_str", "");
		}
		try {
			long fproperty = jsonData.getLongValue("Fproperty");
			String binary = Long.toBinaryString(fproperty);
			char c = binary.charAt(binary.length() - 5);
			mappingData.put("fbindweichat",Integer.parseInt(c + "") );
		} catch (Exception e) {
		}
		return mappingData;
	}
}
