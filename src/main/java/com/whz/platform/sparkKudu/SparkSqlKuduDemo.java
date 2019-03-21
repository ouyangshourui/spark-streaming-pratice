package com.whz.platform.sparkKudu;

import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static java.time.temporal.ChronoField.ALIGNED_WEEK_OF_YEAR;

/**free -
 * @author peiyu
 */
public class SparkSqlKuduDemo {

    private static final String QUERY_SQL = "select \n" +
            "'openApp' as event_id,\n" +
            "CAST(a.fuid as BIGINT) as fuid,\n" +
            "'%s' as dt,\n" +
            "a.fopt_time,\n" +
            "'%d' as years,\n" +
            "'%d' as year_week,\n" +
            "'%d' as y_m_tenofmonth,\n" +
            "'%s' as year_quarter,\n" +
            "'%s' as year_month,\n" +
            "'%d' as numofweek,\n" +
            "'%d' as numofyear,\n" +
            "a.hours,\n" +
            "null as province_id,\n" +
            "a.province as province_name,\n" +
            "null as city_id,\n" +
            "a.city as city_name,\n" +
            "null as district_id,\n" +
            "a.district as district_name,\n" +
            "null as area_id,\n" +
            "null as area_name,\n" +
            "null as subsection_id,\n" +
            "null as subsection_name,\n" +
            "null as store_id,\n" +
            "null as store_name,\n" +
            "null as cate_code1,\n" +
            "null as cate_name1,\n" +
            "null as cate_code2,\n" +
            "null as cate_name2,\n" +
            "null as cate_code3,\n" +
            "null as cate_name3,\n" +
            "null as cate_code4,\n" +
            "null as cate_name4,\n" +
            "null as brandcode,\n" +
            "null as brandname,\n" +
            "null as fskuid,\n" +
            "null as fskutitle,\n" +
            "null as fspuid,\n" +
            "a.ip,\n" +
            "a.coordinate,\n" +
            "a.platformid,\n" +
            "a.appid,\n" +
            "a.platform,\n" +
            "a.biztype,\n" +
            "a.logtype,\n" +
            "a.pagelevelid,\n" +
            "a.viewid,\n" +
            "a.clickid,\n" +
            "a.os,\n" +
            "a.display,\n" +
            "a.downchann,\n" +
            "a.appversion,\n" +
            "a.devicetype,\n" +
            "a.nettype,\n" +
            "null as viewtime\n" +
            "from (\n" +
            "      select a.userid as fuid,\n" +
            "             hour(from_unixtime(floor(cast(a.fronttime as bigint)/1000),'yyyy-MM-dd HH:mm:ss')) as hours,\n" +
            "             from_unixtime(floor(cast(a.fronttime as bigint)/1000),'yyyy-MM-dd HH:mm:ss') as fopt_time,\n" +
            "             a.country,a.province,a.city,a.district,a.ip,a.coordinate,a.servertime, a.platformid, \n" +
            "             a.appid, a.platform,a.biztype, a.logtype, a.pagelevelid, a.viewid, a.viewparam, \n" +
            "             a.clickid, a.clickparam, a.referurl, a.curpageurl,a.os, a.display, a.downchann,\n" +
            "             a.appversion,a.devicetype, a.nettype,a.days\n" +
            "      from fdm.fdm_tracker_detail_da a\n" +
            "      where a.days='2018-05-29' and a.logtype='40000' and a.userid is not null and a.userid<>'' and a.userid<>0\n" +
            ") a";


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("kuduDemo");

        KuduContext kuduContext = new KuduContext("172.172.241.228:7051", SparkContext.getOrCreate());

        SparkSession sparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate();

        LocalDate now = LocalDate.now().minusDays(2);

        String dt = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        int years = now.getYear();
        int year_week = now.get(ALIGNED_WEEK_OF_YEAR);
        int y_m_tenofmonth;
        int dayOfMonth = now.getDayOfMonth();
        if (dayOfMonth <=10) {
            y_m_tenofmonth = 1;
        } else if(dayOfMonth <= 20) {
            y_m_tenofmonth = 2;
        } else {
            y_m_tenofmonth = 3;
        }

        String year_quarter = String.valueOf(years) + "_";

        int month = now.getMonth().getValue();
        if (month <=3) {
            year_quarter += "Q1";
        } else if(month <= 6) {
            year_quarter += "Q2";
        } else if(month <= 9) {
            year_quarter += "Q3";
        } else {
            year_quarter += "Q4";
        }

        String year_month = years + "年" + month + "月";

        int numofweek = now.getDayOfWeek().getValue();
        int numofyear = now.getDayOfYear();


        Dataset<Row> dataset = sparkSession.sql(String.format(QUERY_SQL, dt, years, year_week, y_m_tenofmonth, year_quarter, year_month, numofweek, numofyear));


        kuduContext.upsertRows(dataset, "event_6.event_6_tracker_kudu_40h_2r_de6666");
        System.out.println("------------------------------------OK------------------------------------------");
    }
}
