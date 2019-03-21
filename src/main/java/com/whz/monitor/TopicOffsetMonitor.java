package com.whz.monitor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.whz.platform.sparkKudu.jdbc.JdbcFactory;
import com.whz.platform.sparkKudu.model.JdbcModel;
import com.whz.platform.sparkKudu.util.ResourcesUtil;

public class TopicOffsetMonitor {

	public static void main(String[] args) {
		JdbcModel model = new JdbcModel(
				"monitor",
				ResourcesUtil.getValue("conf", "tidb.driverClassName"),
				ResourcesUtil.getValue("conf", "tidb.url"),
				ResourcesUtil.getValue("conf", "tidb.username"),
				ResourcesUtil.getValue("conf", "tidb.password")
				);
		
		String sql = "select k_appname,sum(k_offset) as sum_offset\r\n" + 
				"from bi_online_offset\r\n" + 
				"where k_date>? and \r\n" + 
				"k_topic =? \r\n" + 
				"GROUP BY k_appname\r\n";
		Date date =new Date();
		SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd HH");
		String time=sdf.format(date);
		Object[] initParams = new Object[]{time,args[0]};
    	List<Map<String, Object>> offsets = JdbcFactory.getJDBCUtil(model).getResultSet(sql,initParams);
    	int max = 0;
    	int min = 0; 
    	int i = 0;
    	for (Map<String, Object> map : offsets) {
    		int sum_offset = (int) map.get("sum_offset");
    		if(i == 0){
    			max = sum_offset;
    			min = sum_offset;
    		}else {
    			if(max < sum_offset) {
    				max = sum_offset;
    			}
    			if(min > sum_offset) {
    				min = sum_offset;
    			}
    		}
			i++;
		}
    	System.out.println("max:" + max + ",min:" + min);
//    	int 
    	
	}

}
