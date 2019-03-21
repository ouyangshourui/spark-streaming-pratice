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
import com.whz.platform.sparkKudu.util.StringUtils;
import com.whz.streaming.base.Kafka2KuduBase;

public class Kafka2KuduUser extends Kafka2KuduBase implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) throws Exception {
		
		Kafka2KuduUser kuduUser = new Kafka2KuduUser();
		kuduUser.setAppname("event_b2b2c_user_kudu")
			.setTopic(ResourcesUtil.getValue("conf", "collect.user.topic"))
			.setGroupId("event_b2b2c_user_kudu")
			.setArgs(args)
			.setBroker(ResourcesUtil.getValue("conf", "collect.metadata.broker.list"))
			.setKuduMaster(ResourcesUtil.getValue("conf", "kudu.master"))
			.setTargetTabel("event.event_user_user")
			.setValidType("t_user_buyer");
		
		kuduUser.transform();
	}

	@Override
	protected JSONObject mappingData(JSONObject jsonData) {
		JSONObject mappingData = new JSONObject();
		mappingData.put("k_ftenancy_id", StringUtils.getVolidString(jsonData.getString("FtenancyId")));
		mappingData.put("k_fid", StringUtils.getVolidString(jsonData.getString("Fuid")));
		mappingData.put("fmid", StringUtils.getVolidString(jsonData.getString("Fmid")));
		mappingData.put("fmobile", StringUtils.getVolidString(jsonData.getString("Fmobile")));
		mappingData.put("femail", StringUtils.getVolidString(jsonData.getString("Femail")));
		mappingData.put("fphoto", StringUtils.getVolidString(jsonData.getString("Fphoto")));
		mappingData.put("faccount_type", StringUtils.getVolidString(jsonData.getString("Faccount_type")));
		mappingData.put("flogin_account", StringUtils.getVolidString(jsonData.getString("Flogin_account")));
		mappingData.put("fusertype", StringUtils.getVolidString(jsonData.getString("Fusertype")));
		mappingData.put("fproperty", StringUtils.getVolidString(jsonData.getString("Fproperty")));
		mappingData.put("fdiffsrcregtime", StringUtils.getVolidString(jsonData.getString("FdiffSrcRegTime")));
		mappingData.put("frating", StringUtils.getVolidString(jsonData.getString("Frating")));
		mappingData.put("fbabyidlist", StringUtils.getVolidString(jsonData.getString("FbabyIdList")));
		mappingData.put("frelationwithbaby", StringUtils.getVolidString(jsonData.getString("FrelationWithBaby")));
		mappingData.put("fnickname", StringUtils.getVolidString(jsonData.getString("Fnickname")));
		mappingData.put("ftruename", StringUtils.getVolidString(jsonData.getString("Ftruename")));
		mappingData.put("fsex", StringUtils.getVolidString(jsonData.getString("Fsex")));
		mappingData.put("fbirthday", StringUtils.getVolidString(jsonData.getString("Fbirthday")));
		mappingData.put("fcommunity", StringUtils.getVolidString(jsonData.getString("Fcommunity")));
		mappingData.put("faddress", StringUtils.getVolidString(jsonData.getString("Faddress")));
		mappingData.put("fpostcode", StringUtils.getVolidString(jsonData.getString("Fpostcode")));
		try {
			String faddtime = jsonData.getString("Faddtime");
			String fdiffSrcRegTime = jsonData.getString("FdiffSrcRegTime");
			if(fdiffSrcRegTime == null || "".equals(fdiffSrcRegTime)) {
				mappingData.put("faddtime", faddtime);
			}else {
				String fregisterSource = jsonData.getString("FregisterSource");
				String[] regs = fdiffSrcRegTime.split(";");
				for(String str : regs) {
					if(fregisterSource.equals(str.split(":")[0])) {
						faddtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(Long.parseLong(str.split(":")[1]) * 1000));
						mappingData.put("faddtime", faddtime);
						break;
					}
				}
			}
		} catch (NumberFormatException e) {
			mappingData.put("faddtime", "");
		}
		mappingData.put("flastupdatetime", StringUtils.getVolidString(jsonData.getString("Flastupdatetime")));
		mappingData.put("freferrer", StringUtils.getVolidString(jsonData.getString("Freferrer")));
		mappingData.put("fmemberlevel", StringUtils.getVolidString(jsonData.getString("FmemberLevel")));
		mappingData.put("fmembercardlist", StringUtils.getVolidString(jsonData.getString("FmembercardList")));
		mappingData.put("frecruiter", StringUtils.getVolidString(jsonData.getString("Frecruiter")));
		mappingData.put("fmanager", StringUtils.getVolidString(jsonData.getString("Fmanager")));
		mappingData.put("fcreator", StringUtils.getVolidString(jsonData.getString("Fcreator")));
		mappingData.put("fcreatordepartment", StringUtils.getVolidString(jsonData.getString("FcreatorDepartment")));
		mappingData.put("fregistersource", StringUtils.getVolidString(jsonData.getString("FregisterSource")));
		mappingData.put("fmembersource", StringUtils.getVolidString(jsonData.getString("FmemberSource")));
		mappingData.put("fdiffchannelactivetime", StringUtils.getVolidString(jsonData.getString("FdiffChannelActiveTime")));
		mappingData.put("fpromoteactive", StringUtils.getVolidString(jsonData.getString("FpromoteActive")));
		mappingData.put("fuserlablelist", StringUtils.getVolidString(jsonData.getString("FuserLableList")));
		mappingData.put("fuserlableremarks", StringUtils.getVolidString(jsonData.getString("FuserLableRemarks")));
		mappingData.put("fmobilestatus", StringUtils.getVolidString(jsonData.getString("FmobileStatus")));
		mappingData.put("fmemberproperty", StringUtils.getVolidString(jsonData.getString("FmemberProperty")));
		mappingData.put("fuserpicturelablelist", StringUtils.getVolidString(jsonData.getString("FuserPictureLableList")));
		mappingData.put("fpregnantplan", StringUtils.getVolidString(jsonData.getString("FpregnantPlan")));
		mappingData.put("fnotename", StringUtils.getVolidString(jsonData.getString("FnoteName")));
		mappingData.put("fabcinviter", StringUtils.getVolidString(jsonData.getString("FabcInviter")));
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
			System.out.println("错误的日期格式：" + fbirthday);
			mappingData.put("fbirthday_next", "");
		}
		mappingData.put("fstatus", StringUtils.getVolidString(jsonData.getString("Fstatus")));
		try {
			mappingData.put("fproperty_str", Long.toBinaryString(jsonData.getLongValue("Fproperty")));
		} catch (Exception e) {
			System.out.println("错误的Fproperty：" + jsonData.getLongValue("Fproperty"));
			mappingData.put("fproperty_str", "");
		}
		try {
			String binary = Long.toBinaryString(jsonData.getLongValue("Fproperty"));
			mappingData.put("fbindweichat", binary.charAt(binary.length() - 5));
		} catch (Exception e) {
			System.out.println("错误的Fproperty：" + jsonData.getLongValue("Fproperty"));
			mappingData.put("fbindweichat", "");
		}
		String fregion = StringUtils.getVolidString(jsonData.getString("Fregion"));
		mappingData.put("fregion", fregion);
		if(fregion != null && !"".equals(fregion)){
			String kuduMaster = ResourcesUtil.getValue("conf", "kudu.master");
			try {
				List<String> columnNames =new ArrayList<>();
				columnNames.add("fprovincename");
				columnNames.add("fcityname");
				columnNames.add("fdistrictname");
				
				Map<String,Object> params = new HashMap<String,Object>();
		        params.put("fprovincesysno", fregion.split("_")[0]);
		        params.put("fcitysysno", fregion.split("_")[1]);
		        params.put("fdistrictsysno", fregion.split("_")[2]);
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
				System.out.println("错误的城市区域信息：" + fregion);
				mappingData.put("fprovincename", "");
				mappingData.put("fcityname", "");
				mappingData.put("fdistrictname", "");
			}
		}
				
		return mappingData;
	}
}
