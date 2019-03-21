package com.whz.streaming;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.util.ResourcesUtil;
import com.whz.platform.sparkKudu.util.StringUtils;
import com.whz.streaming.base.Kafka2KuduBase;

public class Kafka2KuduCoupon extends Kafka2KuduBase implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws Exception {

		Kafka2KuduCoupon kuduUser = new Kafka2KuduCoupon();
		kuduUser.setAppname("event_coupon_t_user_coupon_kudu_de_tmp")
				.setTopic(ResourcesUtil.getValue("conf", "collect.coupon.topic"))
				.setGroupId("event_coupon_t_user_coupon_kudu_de_tmp").setArgs(args)
				.setBroker(ResourcesUtil.getValue("conf", "collect.metadata.broker.list"))
				.setKuduMaster(ResourcesUtil.getValue("conf", "kudu.master"))
				.setTargetTabel("event_tmp.event_tenancy_user_coupon_kudu_de_tmp")// 先用测试表
				.setValidType("t_user_coupon");
		
		kuduUser.transform();
	}

	@Override
	protected JSONObject mappingData(JSONObject jsonData) {

		JSONObject mappingData = new JSONObject();

		String fget_time = StringUtils.getVolidString(jsonData.getString("Fget_time"));// 1
		String fuse_time = StringUtils.getVolidString(jsonData.getString("Fuse_time"));// 3
		String freturn_time = StringUtils.getVolidString(jsonData.getString("Freturn_time"));// 4

		try {
			int type = compareTime(fget_time, fuse_time, freturn_time);
			if (type == 1) {
				opTimefun(mappingData, StringUtils.getVolidString(jsonData.getString("Fget_time")));
				mappingData.put("k_fid", StringUtils.getVolidString(jsonData.getString("Fuser_id")));
				mappingData.put("fop_type", 1);// 1 领券
			} else if (type == 3) {
				opTimefun(mappingData, StringUtils.getVolidString(jsonData.getString("Fuse_time")));
				mappingData.put("k_fid", StringUtils.getVolidString(jsonData.getString("Fuse_user_id")));
				mappingData.put("fop_type", 3);// 3 用券
			} else if (type == 4) {
				opTimefun(mappingData, StringUtils.getVolidString(jsonData.getString("Freturn_time")));
				mappingData.put("k_fid", StringUtils.getVolidString(jsonData.getString("Fuser_id")));
				mappingData.put("fop_type", 4);// 4 取消返还
			}

		} catch (ParseException e) {
			mappingData.put("fop_type", 1);
			mappingData.put("k_fid", "");
			mappingData.put("k_fopt_time", "");
			mappingData.put("k_dt", "");
			mappingData.put("k_year", "");
			mappingData.put("k_year_quarter", "");
			mappingData.put("k_year_month", "");
			mappingData.put("k_year_week", "");
			mappingData.put("k_y_m_tenofmonth", "");
			mappingData.put("k_hours", "");
			mappingData.put("k_year_week_day", "");
			System.out.println("-----时间解析错误----");
			e.printStackTrace();
		}

		mappingData.put("k_ftenancy_id", StringUtils.getVolidString(jsonData.getString("Fplatform_id")));
		mappingData.put("fresource_batch_id", StringUtils.getVolidString(jsonData.getString("Fresource_batch_id")));
		mappingData.put("fcoupon_code", StringUtils.getVolidString(jsonData.getString("Fcoupon_code")));
		mappingData.put("fsend_platform", StringUtils.getVolidString(jsonData.getString("Fsend_platform")));
		mappingData.put("fget_time", StringUtils.getVolidString(jsonData.getString("Fget_time")));
		mappingData.put("fend_use_time", StringUtils.getVolidString(jsonData.getString("Fend_use_time")));
		mappingData.put("fsale_amt", StringUtils.getVolidString(jsonData.getString("Fsale_amt")));
		mappingData.put("fuse_entity_id", StringUtils.getVolidString(jsonData.getString("Fuse_entity_id")));
		mappingData.put("fuse_bdeal_code", StringUtils.getVolidString(jsonData.getString("Fuse_bdeal_code")));
		mappingData.put("fno_user", StringUtils.getVolidString(jsonData.getString("Fno_user")));
		mappingData.put("fsource_id", StringUtils.getVolidString(jsonData.getString("Fsource_id")));
		mappingData.put("fsend_business", StringUtils.getVolidString(jsonData.getString("Fsend_business")));
		mappingData.put("fpublic_coupon_code", StringUtils.getVolidString(jsonData.getString("Fpublic_coupon_code")));
		mappingData.put("fcoupon_name", StringUtils.getVolidString(jsonData.getString("Fcoupon_name")));
		mappingData.put("fplatform_id", StringUtils.getVolidString(jsonData.getString("Fplatform_id")));
		mappingData.put("fcoupon_type", StringUtils.getVolidString(jsonData.getString("Fcoupon_type")));
		mappingData.put("fend_unavailable_time",
				StringUtils.getVolidString(jsonData.getString("Fend_unavailable_time")));
		mappingData.put("fcoupon_amt", StringUtils.getVolidString(jsonData.getString("Fcoupon_amt")));
		mappingData.put("fstart_unavailable_time",
				StringUtils.getVolidString(jsonData.getString("Fstart_unavailable_time")));
		mappingData.put("fsite_id", StringUtils.getVolidString(jsonData.getString("Fsite_id")));
		mappingData.put("fuse_platform", StringUtils.getVolidString(jsonData.getString("Fuse_platform")));
		mappingData.put("fget_bdeal_code", StringUtils.getVolidString(jsonData.getString("Fget_bdeal_code")));
		mappingData.put("fget_mobile", StringUtils.getVolidString(jsonData.getString("Fget_mobile")));
		mappingData.put("freturn_time", StringUtils.getVolidString(jsonData.getString("Freturn_time")));
		mappingData.put("fuse_user_id", StringUtils.getVolidString(jsonData.getString("Fuse_user_id")));
		mappingData.put("fis_cash_coupon", StringUtils.getVolidString(jsonData.getString("Fis_cash_coupon")));
		mappingData.put("fuse_time", StringUtils.getVolidString(jsonData.getString("Fuse_time")));
		mappingData.put("fstart_use_time", StringUtils.getVolidString(jsonData.getString("Fstart_use_time")));
		mappingData.put("fuse_mobile", StringUtils.getVolidString(jsonData.getString("Fuse_mobile")));
		mappingData.put("fsend_person", StringUtils.getVolidString(jsonData.getString("Fsend_person")));
		mappingData.put("fcoupon_barcode", StringUtils.getVolidString(jsonData.getString("Fcoupon_barcode")));
		mappingData.put("fstate", StringUtils.getVolidString(jsonData.getString("Fstate")));

		return mappingData;
	}

	private void opTimefun(JSONObject mappingData, String fopt_time) throws ParseException {
		String value = null;
		if (fopt_time != null && !"".equals(fopt_time)) {
			mappingData.put("k_fopt_time", fopt_time);

			value = fopt_time.split(" ")[0];
			mappingData.put("k_dt", value);

			value = fopt_time.split(" ")[0].split("\\-")[0];
			mappingData.put("k_year", value);

			String[] d = fopt_time.split(" ")[0].split("\\-");
			int month = Integer.valueOf(d[1]);
			if (month < 4) {
				value = d[0] + "-" + "Q1";
			} else if (month < 7) {
				value = d[0] + "-" + "Q2";
			} else if (month < 10) {
				value = d[0] + "-" + "Q3";
			} else if (month < 13) {
				value = d[0] + "-" + "Q4";
			}
			mappingData.put("k_year_quarter", value);

			d = fopt_time.split(" ")[0].split("\\-");
			if (d[1].length() > 1) {
				value = d[0] + "-" + d[1];
			} else {
				value = d[0] + "-0" + d[1];
			}
			mappingData.put("k_year_month", value);

			value = getSeqWeek(fopt_time);
			mappingData.put("k_year_week", value);

			d = fopt_time.split(" ")[0].split("\\-");
			int day = Integer.valueOf(d[2]);
			String month2;
			if (d[1].length() > 1) {
				month2 = d[1];
			} else {
				month2 = "0" + d[1];
			}
			if (day < 11) {
				value = d[0] + "-" + month2 + "-" + "T1";
			} else if (day < 21) {
				value = d[0] + "-" + month2 + "-" + "T2";
			} else if (day < 32) {
				value = d[0] + "-" + month2 + "-" + "T3";
			}
			mappingData.put("k_y_m_tenofmonth", value);

			String hour = fopt_time.split(" ")[1].split(":")[0];
			if (hour.length() > 1) {
				value = hour;
			} else {
				value = "0" + hour;
			}
			mappingData.put("k_hours", value);
			value = getSeqWeek(fopt_time);
			value += "-" + getDateToWeek(fopt_time);
			mappingData.put("k_year_week_day", value);
		}

	}

	/**
	 * 产生周序列,即得到当前时间所在的年度是第几周
	 * 
	 * @return String
	 * @throws ParseException
	 */
	public static int getDateToWeek(String date) throws ParseException {
		try {
			Calendar c = Calendar.getInstance();
			c.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date));
			int w = c.get(Calendar.DAY_OF_WEEK) - 1; // 指示一个星期中的某天。
			if (w == 0)
				w = 7;
			return w;
		} catch (Exception e) {
			System.out.println("日期类型有误：" + date);
			return 0;
		}
	}

	/**
	 * 产生周序列,即得到当前时间所在的年度是第几周
	 * 
	 * @return String
	 * @throws ParseException
	 */
	public static String getSeqWeek(String date) throws ParseException {
		try {
			Calendar c = Calendar.getInstance();
			c.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date));
			String week = Integer.toString(c.get(Calendar.WEEK_OF_YEAR));
			if (week.length() > 1) {
				week = "W" + week;
			} else {
				week = "W0" + week;
			}
			String year = Integer.toString(c.get(Calendar.YEAR));
			return year + "-" + week;
		} catch (Exception e) {
			System.out.println("日期类型有误：" + date);
			return "";
		}
	}

	private int compareTime(String fget_time, String fuse_time, String freturn_time) {

		int flag = 1;
		if (org.apache.commons.lang.StringUtils.isEmpty(fget_time)) {
			fget_time = "";
		}
		if (org.apache.commons.lang.StringUtils.isEmpty(fuse_time)) {
			fuse_time = "";
		}
		if (org.apache.commons.lang.StringUtils.isEmpty(freturn_time)) {
			freturn_time = "";
		}

		String[] str = { fget_time, fuse_time, freturn_time };
		String max = str[0];
		for (int i = 0; i < str.length; i++) {
			if (str[i].compareTo(max) > 0) {
				max = str[i];
				if (i == 1) {
					flag = 3;
				} else if (i == 2) {
					flag = 4;
				}
			}

		}
		return flag;
	}
}
