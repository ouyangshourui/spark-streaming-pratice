package com.whz.platform.sparkKudu.hbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.model.HbaseCell;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Description hbase reader
 * @Author ourui
 * @Date 2019/02/22
 **/
public class HBaseReader extends RichSourceFunction<JSONObject> implements Serializable {
    private static final long serialVersionUID = 3L;
    private static final Logger logger = LoggerFactory.getLogger(HBaseReader.class);

    private Connection conn = null;
    private Scan scan = null;

    public void setZookeeper(String zookeeper) {
        this.zookeeper = zookeeper;
    }

    private String zookeeper=null;


    private  Table table=null;


    public void setTableName(String tableName) throws IOException {
        this.tableName = tableName;
    }

    public void setColFamily(String colFamily) {
        this.colFamily = colFamily;
    }

    private String tableName=null;
    private String colFamily=null;


    @Override
    public void open(Configuration parameters) throws Exception {
        HBaseConfiguration conf = new HBaseConfiguration();
        conf.set("hbase.zookeeper.quorum", zookeeper);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent","/hbase-unsecure");

    }

    @Override
    public void run(SourceContext<JSONObject> ctx) throws Exception {
        while(true){
            try {
                HashMap<String,String> columnMap= new HashMap();
                List<List<HbaseCell>> listrow=  HBaseUtil.getInstance().queryLimit(tableName,colFamily,1000);
                for (List<HbaseCell> celllist : listrow) {
                    Map<String, String> bTable_map = new HashMap();
                    for (HbaseCell hbasecell : celllist) {
                        JSONObject jsondata= new JSONObject();
                        columnMap.put("rowkey",hbasecell.getRowkey());
                        columnMap.put(hbasecell.getColumn(), (String) hbasecell.getValue());
                    }
                    JSONObject itemJSONObj = JSONObject.parseObject(JSON.toJSONString(columnMap));
                    ctx.collect(itemJSONObj);
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                TimeUnit.MILLISECONDS.sleep(10*1000);
            }

        }

    }

    @Override
    public void cancel() {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            logger.error("Close HBase Exception:", e.toString());
        }

    }
}

