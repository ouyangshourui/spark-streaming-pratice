package sparkKudu;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSONObject;

public class Test {

	public static int dateToWeek(String datetime) {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        String[] weekDays = {  "星期一", "星期二", "星期三", "星期四", "星期五", "星期六" ,"星期日"};
        Calendar cal = Calendar.getInstance(); // 获得一个日历
        Date datet = null;
        try {
            datet = f.parse(datetime);
            cal.setTime(datet);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        int w = cal.get(Calendar.DAY_OF_WEEK) - 1; // 1指示一个星期中的某天。
        if (w == 0)
            w = 7;
        return w;
    }

    public static void main(String[] args) {
//        System.out.println("2018-09-01".compareTo("2018-09-02"));
//    	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
//    	Calendar c = Calendar.getInstance();
//    	c.setTime(new Date());
//    	c.add(Calendar.MONTH, -2);
//    	Date m = c.getTime();
//    	String mon = format.format(m) + "-01";
//    	System.out.println("过去一个月："+mon);
//    	int index = "123456789jfsdf".length() - 4;
//    	System.out.println(index);
//    	System.out.println("123456789jfsdf".charAt(index));
    	
//    	Pattern pattern = Pattern.compile("sfss(_)?[0-9]*");
//	    Matcher isNum = pattern.matcher("sfss");
//	    if (!isNum.matches()) {
//	    	System.out.println(false);
//	    }else{
//	    	System.out.println(true);
//	    }
//    	Map<String,Object> json = new HashMap<>();
//    	
//    	json.put("ss", null);
//    	
//    	System.out.println(json);
//    	
//    	JSONObject js = new JSONObject();
//    	js.put("ss", "");
//    	js.put("sdds", null);
//    	System.out.println(js);
//    	System.out.println("2018-01-01".split("\\-")[1]);
    	
    	
//    	System.out.println(new Date().getTime());
//    	while(true) {
//    		System.out.println(new Random().nextInt(4));
//    	}
//    	String s = "2019-01-20 18:16:46	1547979394117	1.204.118.126	410c0c5513456b75	1006277192	100119	1	02	89f49bb6cac24ee40e108242de4b787a	3	-1	001	10000			13000	10001	{entityid:1024	skuid:2781163}			001	40000				11.014	\\N	\\N	\\N	\\N	\\N	Android8.0.0	1080*2076		110	samsung-SM-G9600	wifi			4wr8ql9812zc0lxye3tqjtl5w						18	10	10001	2019-01-20";
//    	System.out.println(s.split("\t").length);
//    	
//    	for(int i =0;i< s.split("\t").length;i++)System.out.println(s.split("\t")[i]);
    	
//    	System.out.println(Long.parseLong("0000"));
    	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
    	Calendar c = Calendar.getInstance();
    	c.setTime(new Date());
    	if(c.get(Calendar.DAY_OF_MONTH) > 25) {
    		c.add(Calendar.MONTH, -1);
    	}else {
    		c.add(Calendar.MONTH, -2);
    	}
    	Date m = c.getTime();
    	String mon = format.format(m) + "-01";
    	
    	System.out.println(mon);
    }

}
