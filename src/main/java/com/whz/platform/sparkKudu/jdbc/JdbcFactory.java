/**
 * @Title: JdbcFactory.java
 * @Package com.anhouse.datahouse.util
 * @author steven
 * @date 2016年3月23日 上午11:02:12
 */
package com.whz.platform.sparkKudu.jdbc;

import com.whz.platform.sparkKudu.model.JdbcModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author steven
 * @ClassName: JdbcFactory
 * @date 2016年3月23日 上午11:02:58
 */
public class JdbcFactory {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcFactory.class);

    private static ConcurrentHashMap<String, JdbcUtils> jdbcMap;

    private static int try_count = 0;
    
    static {
        jdbcMap = new ConcurrentHashMap<String, JdbcUtils>();
    }

    private JdbcFactory() {
        super();
    }

    public synchronized static JdbcUtils getJDBCUtil(JdbcModel model) {
        if (jdbcMap == null) {
            jdbcMap = new ConcurrentHashMap<String, JdbcUtils>();
        }
        String databaseKey = model.getDatabaseKey() + "_" + new Random().nextInt(5);
        JdbcUtils ju = jdbcMap.get(databaseKey);
        try {
            if (ju == null || !ju.hasConn() || ju.isConnClosed()) {
                ju = new JdbcUtils(model);
                jdbcMap.put(databaseKey, ju);
                //重置重试次数
                try_count = 0;
                return ju;
            }else{
            	try {
					ju.excuteSql("select 1");
//					LOG.info(Thread.currentThread().getName() + " 数据源可用");
					return ju;
				} catch (Exception e) {
					if(try_count > 3){
						throw new RuntimeException("获取数据源连接异常,重试超过3次，终止程序！！");
					}
					Thread.sleep(2000);
					try_count++;
					ju.closeCon();
					jdbcMap.remove(databaseKey);
					LOG.info(Thread.currentThread().getName() + " 数据源已经失效 重新生成数据源 try_count:" + try_count);
					return getJDBCUtil(model);
				}
            }
        } catch (Exception e) {
        	e.printStackTrace();
        	System.out.println(e.getMessage());
            throw new RuntimeException("获取数据源连接异常");
        }
    }

    /**
     * @return
     */
    public static void closeAllCon() {
        if (jdbcMap == null) {
            return;
        }
        //循环所有key
        for (JdbcUtils value : jdbcMap.values()) {
            value.closeCon();
        }
    }
}