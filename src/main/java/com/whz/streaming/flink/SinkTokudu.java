package com.whz.streaming.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.kudu.KuduUtil;
import com.whz.platform.sparkKudu.util.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SinkTokudu extends RichSinkFunction<ArrayList<Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, String>>>> {

        private  static final String tableName="event.real_time_event_tenancy_user_sku_order_kudu_de";
    private  static  final String kudumaster="10.253.240.11,10.253.240.16,10.253.240.14";


    /** 每条数据的插入都要调用一次 invoke() 方法
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(ArrayList<Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, String>>> value, Context context) {

      for (Tuple2<Tuple9<String, String, String, String, String, String, String, String, String>, Map<String, String>> data:value) {
          //System.out.println(value.f0);
          try {
              Tuple9<String, String, String, String, String, String, String, String, String> key = data.f0;
              JSONObject json = JSONObject.parseObject(JSON.toJSONString(data.f1));
              KuduUtil kuduInstance = KuduUtil.getKuduInstance(kudumaster);
              List<ColumnSchema> columns = kuduInstance.getTableColumns(tableName).getColumns();
              List<Map<String, Object>> list = new ArrayList<>();
              for (ColumnSchema schema : columns) {
                  Map<String, Object> map = new HashMap<>();
                  map.put("column", schema.getName());
                  map.put("type", schema.getType());
                  list.add(map);
              }
              Map<String, Object> mdata = mappingData(list, json, key);
              System.out.println("write2kudu"+key);
              kuduInstance.updateColumn(tableName, mdata);
          } catch (ParseException e) {
              e.printStackTrace();
          } catch (KuduException e) {
              e.printStackTrace();
          } catch (Exception e) {
              e.printStackTrace();
          } finally {
          }
      }

    }

    public  Map<String,Object> mappingData(List<Map<String, Object>> columnList, JSONObject jsonData,Tuple9<String, String, String, String, String, String, String, String, String> key) throws ParseException {
        Map<String,Object> mappingData = new Hashtable<>() ;
        mappingData.put("etl_date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

        for(Map<String, Object> column : columnList){
            String columnName = column.get("column").toString();
            //kudu默认3个月归档 系统动态获取3个月前的第一天
            String miniRanger = kuduMiniRanger();
//			System.out.println("miniRanger:" + miniRanger + ",saleTime:"+jsonData.getString("saleTime"));
            //如果 上报的数据日期为空或者 小于kudu表的最小分区 则过滤数据
            String event_day_time=key.f1;
            String _fopt_time = key.f3;
            if("".equals(event_day_time) || miniRanger.compareTo(event_day_time)> 0){
                return mappingData;
            }
            if("k_ftenancy_id".equals(columnName)){
                mappingData.put(columnName, key.f0);
            }else if("k_dt".equals(columnName)){

                mappingData.put(columnName, event_day_time);
            }else if("k_fid".equals(columnName)){

                mappingData.put(columnName, key.f2);
            }else if("k_fopt_time".equals(columnName)){
                mappingData.put(columnName, key.f3);
            }else if("ftrade_id".equals(columnName)){
                mappingData.put(columnName, key.f4);
            }else if("fdeal_id".equals(columnName)){
                mappingData.put(columnName, key.f5);
            }else if("fskuid".equals(columnName)){
                mappingData.put(columnName, key.f6);
            } else if("fbdeal_id".equals(columnName)){
                mappingData.put(columnName, key.f7);
            } else if("fopt_type".equals(columnName)){
                mappingData.put(columnName, key.f8);
            } else if("k_year".equals(columnName)){
                String value = null;
                if(_fopt_time != null){
                    value = _fopt_time.split(" ")[0].split("\\-")[0];
                }
                mappingData.put(columnName, value);
            }else if("k_year_quarter".equals(columnName)){
                String value = null;
                if(_fopt_time != null){
                    String[] d = _fopt_time.split(" ")[0].split("\\-");
                    int month = Integer.valueOf(d[1]);
                    if(month < 4){
                        value = d[0] + "-" + "Q1";
                    }else if(month < 7){
                        value = d[0] + "-" + "Q2";
                    }else if(month < 10){
                        value = d[0] + "-" + "Q3";
                    }else if(month < 13){
                        value = d[0] + "-" + "Q4";
                    }
                }
                mappingData.put(columnName, value);
            }else if("k_year_month".equals(columnName)){
                String value = null;
                if(_fopt_time != null){
                    String[] d = _fopt_time.split(" ")[0].split("\\-");
                    if(d[1].length() > 1){
                        value = d[0] + "-" + d[1];
                    }else{
                        value = d[0] + "-0" + d[1];
                    }
                }
                mappingData.put(columnName, value);
            }else if("k_year_week".equals(columnName)){
                String value = null;
                if(_fopt_time != null){
                    value = getSeqWeek(_fopt_time);
                }
                mappingData.put(columnName, value);
            }else if("k_y_m_tenofmonth".equals(columnName)){
                String value = null;
                if(_fopt_time != null){
                    String[] d = _fopt_time.split(" ")[0].split("\\-");
                    int day = Integer.valueOf(d[2]);
                    String month;
                    if(d[1].length() > 1){
                        month = d[1];
                    }else{
                        month = "0" + d[1];
                    }
                    if(day < 11){
                        value = d[0] + "-" + month + "-" + "T1";
                    }else if(day < 21){
                        value = d[0] + "-" + month + "-" + "T2";
                    }else if(day < 32){
                        value = d[0] + "-" + month + "-" + "T3";
                    }
                }
                mappingData.put(columnName, value);
            }else if("k_hours".equals(columnName)){
                String value = null;
                if(_fopt_time != null){
                    String hour = _fopt_time.split(" ")[1].split(":")[0];
                    if(hour.length() > 1){
                        value = hour;
                    }else{
                        value = "0" + hour;
                    }
                }
                mappingData.put(columnName, value);
            }else if("k_year_week_day".equals(columnName)){
                String value = null;
                if(_fopt_time != null){
                    value = getSeqWeek(_fopt_time);

                    value += "-" + getDateToWeek(_fopt_time);
                }
                mappingData.put(columnName, value);
            }else if("fplatform_id".equals(columnName)){
                mappingData.put("fplatform_id", StringUtils.getVolidString(jsonData.getString("fplatform_id")));
            }

        }
//        mappingData.put("k_ftenancy_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("k_dt", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("k_fid", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("k_fopt_time", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fskuid", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fbdeal_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fopt_type", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("k_year", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("k_year_quarter", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("k_year_month", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("k_year_week", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("k_y_m_tenofmonth", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("k_year_week_day", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("k_hours", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("etl_date", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fplatform_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fchannel_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fentity_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_type", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_source", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fstore_name", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_state", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_produce_state", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fseller_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fseller_shop_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fseller_name", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fseller_shop_name", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fsite_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_pay_type", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_obtain_experience", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_sale_mode", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_refund_state", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("frecv_name", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("frecv_province_code", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("frecv_city_code", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("frecv_region_code", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("frecv_area_code", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fprovincename", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fcityname", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdistrictname", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("frecv_address", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("frecv_post_code", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("frecv_mobile", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_storage_from", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_storage_type", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_delivery_type", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fexpress_type", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fexpress_company_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fexpress_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fextenal_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("finvoice_head", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("finvoice_fee", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_eval_state", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_del_flag", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_visible_state", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fstore_io", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_gen_time", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_pay_suss_time", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_cancel_time", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_consign_time", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fdeal_recv_confirm_time", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_num", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fitem_weighing_code", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_obtain_experience", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_sale_mode", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_output_tax", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_output_tax_rate", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fitem_cost_price", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fitem_origin_price", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fitem_sold_price", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_total_fee", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_down_fee", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_overseas_tax_free", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_total_payment", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_down_payment", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_coupon_payment", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_score_payment", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_prepaid_payment", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_discount_payment", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_shipping_share", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_is_score", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_score_fee", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_invoice_fee", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_coupon_id", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_tax_ticket_no", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_special_price", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("ftrade_card_payment", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fskubrand", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fskubrandname", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("erp_brandid", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("erp_brand_name", StringUtils.getVolidString(jsonData.getString("")));
//        mappingData.put("fskutitle", StringUtils.getVolidString(jsonData.getString("")));
        return mappingData;

    }

    @Override
    public void close() throws Exception {
    }

    /**
     * 产生周序列,即得到当前时间所在的年度是第几周
     * @return String
     * @throws ParseException */
    public static String getSeqWeek(String date) throws ParseException {
        try {
            Calendar c = Calendar.getInstance();
            c.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date));
            String week = Integer.toString(c.get(Calendar.WEEK_OF_YEAR));
            if(week.length() > 1){
                week = "W" + week;
            }else{
                week = "W0" + week;
            }
            String year = Integer.toString(c.get(Calendar.YEAR));
            return year + "-" + week;
        } catch (Exception e) {
            System.out.println("日期类型有误：" + date);
            return "";
        }
    }

    /**
     * 产生周序列,即得到当前时间所在的年度是第几周
     * @return String
     * @throws ParseException */
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

    private static String kuduMiniRanger(){
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.MONTH, -2);
        Date m = c.getTime();
        String mon = format.format(m) + "-01";
        return mon;
    }


    /**
     * @param masters
     * @param tableName
     * @return
     * @throws Exception
     */
    private static List<Map<String,Object>> initKudu(String masters,  String tableName) throws Exception{

        KuduUtil kuduInstance=KuduUtil.getKuduInstance(masters);

        //System.out.println("kudu 初始化成功！");
        List<ColumnSchema> columns = kuduInstance.getTableColumns(tableName).getColumns();
        //System.out.println("kudu table[" + tableName + "] 初始化成功！");

      //  List<StructField> structFields = new ArrayList<>();

        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        for (ColumnSchema schema : columns) {
            Map<String,Object> map = new HashMap<String,Object>();
           // structFields.add(parse(schema));
            map.put("column", schema.getName());
            map.put("type", schema.getType());
            list.add(map);
        }

        //structType = DataTypes.createStructType(structFields);


        return list;
    }


}
