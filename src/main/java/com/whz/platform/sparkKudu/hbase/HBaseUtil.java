package com.whz.platform.sparkKudu.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.whz.platform.sparkKudu.model.HbaseCell;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;

public class HBaseUtil {
	private static Logger log = Logger.getLogger(HBaseUtil.class);
	private static HBaseUtil util;
	private Connection conn;
	private static String zookeeper;

	public static void setZookeeper(String server) {
		zookeeper = server;
	}


	public synchronized static HBaseUtil getInstance() throws IOException {
		if(util == null) {
			util = new HBaseUtil();
		}else {
			if(util.conn.isClosed() || util.conn.isAborted()) {
				if(util.conn != null) {
					util.conn.close();
				}
				util = new HBaseUtil();
			}
		}
		return util;
	}
	
	private HBaseUtil() throws IOException {
		super();
		initHbaseConection();
	}


	/**
	 * 获取hbase链接
	 * @param model
	 * @return
	 * @throws IOException
	 */
	private void initHbaseConection() throws IOException{
		Configuration conf = HBaseConfiguration.create(); 
		conf.set("hbase.zookeeper.quorum", zookeeper);
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");
        conn = ConnectionFactory.createConnection(conf);
        log.info("hbase链接获取成功！");
    }

//	/**
//	 * 获取hbase链接
//	 * @param model
//	 * @return
//	 * @throws IOException
//	 */
//	private void initHbaseConection(String zookeeper) throws IOException{
//		HBaseConfiguration conf = new HBaseConfiguration();
//		Configuration conf = HBaseConfiguration.create(); 
//		conf.set("hbase.zookeeper.quorum", zookeeper);
//		conf.set("hbase.zookeeper.property.clientPort", "2181");
//		conf.set("zookeeper.znode.parent","/hbase-unsecure");
//		conn = ConnectionFactory.createConnection(conf);
//		log.info("hbase链接获取成功！");
//	}

	/**
	 * 获取hbase链接
	 * @param model
	 * @return
	 * @throws IOException
	 */

	public Connection getConn(){
		return conn;
	}
    
    /**
     * 
     * @param tableName
     * @param columnFamilys
     */
    public void createTable(String tableName, String[] columnFamilys)throws IOException { 
        // 创建一个数据库管理员  
        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();  
        BasicConfigurator.configure();
        if (hAdmin.tableExists(tableName)) {  
        	log.info(tableName + "表已存在");  
        } else {  
            // 新建一个表描述  
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));  
            // 在表描述里添加列族  
            for (String columnFamily : columnFamilys) {
				tableDesc.addFamily(new HColumnDescriptor(columnFamily).setCompressionType(Compression.Algorithm.SNAPPY));
            }  
            // 根据配置好的表描述建表  
            hAdmin.createTable(tableDesc);  
            log.info("创建" + tableName + "表成功");  
        }  
        hAdmin.close();
    }

	/**
	 *
	 * @param tableName
	 *
	 */
	public void dropTable(String tableName)throws IOException {
		// 创建一个数据库管理员
		HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();
		BasicConfigurator.configure();
		if (!hAdmin.tableExists(tableName)) {
			log.info(tableName + "表不存在");
		}
		hAdmin.disableTable(tableName);
		hAdmin.deleteTable(tableName);

		hAdmin.close();
	}

	/**
	 *
	 * @param tableName
	 *
	 */
	public Boolean existTable(String tableName)throws IOException {
		// 创建一个数据库管理员
		HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();
		Boolean tableExists=hAdmin.tableExists(tableName);
		hAdmin.close();
		return tableExists;
	}

	/**
     * 
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     */
    public void insertData(String tableName, String rowKey, String columnFamily, String column, String value)throws IOException { 
//    	long start = System.currentTimeMillis();
        // 获取表  
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
		//通过rowkey创建一个put对象
        Put put = new Put(Bytes.toBytes(rowKey));  
        // 在put对象中设置列族、列、值    // 插入数据,可通过put(List<Put>)批量插入  
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));  
        table.put(put);
        table.flushCommits();
		table.close();
        //System.out.println(System.currentTimeMillis() - start);
    }
    
    /**
     * 
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     */
    public void insertData(String tableName, String rowKey, String columnFamily, JSONObject values)throws IOException { 
//    	long start = System.currentTimeMillis();
        // 获取表  
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
		//通过rowkey创建一个put对象
        Put put = new Put(Bytes.toBytes(rowKey));  
        // 在put对象中设置列族、列、值    // 插入数据,可通过put(List<Put>)批量插入  
        for(String key : values.keySet()) {
        	put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(key), Bytes.toBytes(values.getString(key)));  
        }
        table.put(put);
//        table.flushCommits();
		table.close();
//        System.out.println(System.currentTimeMillis() - start);
    }


	/**
	 *
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param column
	 * @param value
	 */
	public void insertData(String tableName, String rowKey, String columnFamily, Map<String,String> values)throws IOException {
//    	long start = System.currentTimeMillis();
		// 获取表
		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
		//通过rowkey创建一个put对象
		Put put = new Put(Bytes.toBytes(rowKey));
		// 在put对象中设置列族、列、值    // 插入数据,可通过put(List<Put>)批量插入
		for(String key : values.keySet()) {
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(key), Bytes.toBytes(values.get(key)));
		}
		table.put(put);
//        table.flushCommits();
		table.close();
//        System.out.println(System.currentTimeMillis() - start);
	}


	/**
	 *
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param column
	 * @param value
	 */
	public void insertData(String tableName, String columnFamily, Map<String,Map<String,String>> mapdata)throws IOException {
//    	long start = System.currentTimeMillis();
		// 获取表
		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
		//通过rowkey创建一个put对象
		List<Put> pList=new ArrayList<Put>();
		for(String rowKey:mapdata.keySet()){
			Put put = new Put(Bytes.toBytes(rowKey));
			// 在put对象中设置列族、列、值    // 插入数据,可通过put(List<Put>)批量插入
			Map<String, String> value = mapdata.get(rowKey);
			for(String key : value.keySet()) {
				put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(key), Bytes.toBytes(value.get(key)));
			}
			pList.add(put);
			if(pList.size()%10000==0) {
				table.put(pList);
				pList=new ArrayList<Put>();
			}

		}

		table.put(pList);
        table.flushCommits();
		table.close();
//        System.out.println(System.currentTimeMillis() - start);
	}


	/**
	 *
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param column
	 * @param value
	 */
	public void insertData(HTable table, String rowKey, String columnFamily, String column, String value)throws IOException {
		long start = System.currentTimeMillis();
		// 获取表
		//通过rowkey创建一个put对象
		Put put = new Put(Bytes.toBytes(rowKey));
		// 在put对象中设置列族、列、值    // 插入数据,可通过put(List<Put>)批量插入
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		table.put(put);
		// 关闭资源
		System.out.println(System.currentTimeMillis() - start);
	}

	/**
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public HTable getTableHTable  (String tableName)throws IOException {

		return  (HTable) conn.getTable(TableName.valueOf(tableName));

	}

	/**
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public void closeTableHTable  (HTable bTable)throws IOException {

		bTable.close();

	}




	/**
     * 删除一条数据  
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public void delRow(String tableName, String rowKey) throws IOException {  
        // 获取表  
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));  
        // 删除数据  
        Delete delete = new Delete(Bytes.toBytes(rowKey));  
        table.delete(delete);  
        // 关闭资源  
        table.close();  
    }  
    
    /**
     * // 删除多条数据  
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public void delRows(String tableName, String[] rows) throws IOException {  
        // 获取表  
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));  
        // 删除多条数据  
        List<Delete> list = new ArrayList<Delete>();  
        for (String row : rows) {  
            Delete delete = new Delete(Bytes.toBytes(row));  
            list.add(delete);  
        }  
        table.delete(list);  
        // 关闭资源  
        table.close();  
    }  
    
    /**
     * 
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public void delColumnFamily(String tableName, String columnFamily) throws IOException {  
        // 创建一个数据库管理员  
        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();  
        // 删除一个表的指定列族  
        hAdmin.deleteColumn(tableName, columnFamily);  
        
        hAdmin.close();
    }
	
    /**
     *  
     * @param tableName 表名
     * @param columnFamily 列簇
     * @param rowKey rowkey
     * @param column 列
     * @throws IOException
     */
    public List<HbaseCell> getColumnInfo(String tableName, String columnFamily, String rowKey, String column) throws IOException {
        // 获取表  
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));  
        // 通过rowkey创建一个get对象  
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        // 输出结果  
        Result result = table.get(get);  
        List<HbaseCell> cellList = new ArrayList<HbaseCell>();
        for (Cell cell : result.rawCells()) {  
        	HbaseCell hbase = new HbaseCell(new String(CellUtil.cloneRow(cell)),
        			new String(CellUtil.cloneFamily(cell)),
        			new String(CellUtil.cloneQualifier(cell)),
        			Bytes.toString(CellUtil.cloneValue(cell)),
        			cell.getTimestamp());
        	cellList.add(hbase);
        }  
        // 关闭资源  
        table.close();  
        
        return cellList;
    }


	/**
	 * @param tableName
	 * @param columnFamily
	 * @param rowKeys
	 * @return
	 * @throws IOException
	 */
	public Map<String,List<HbaseCell>>  getRow(String tableName, String columnFamily, ArrayList<String> rowKeys) throws IOException {
		// 获取表
		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
		List<Get> list = new ArrayList<>();
		Map<String,List<HbaseCell>>  data = new HashMap<>();
		// 通过rowkey创建一个get对象
		for(String rowKey:rowKeys){
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addFamily(Bytes.toBytes(columnFamily));
			list.add(get);
		}

		// 输出结果
		Result[] results = table.get(list);
		for(Result result:results){
			List<HbaseCell> cellList = new ArrayList<HbaseCell>();
			for (Cell cell : result.rawCells()) {
				HbaseCell hbase = new HbaseCell(new String(CellUtil.cloneRow(cell)),
						new String(CellUtil.cloneFamily(cell)),
						new String(CellUtil.cloneQualifier(cell)),
						Bytes.toString(CellUtil.cloneValue(cell)),
						cell.getTimestamp());
				cellList.add(hbase);
			}

			data.put(new String(result.getRow()),cellList);
		}


		// 关闭资源
		table.close();

		return data;
	}
	/**
     *  
     * @param tableName
     * @param columnFamily
     * @param rowKey
     * @throws IOException
     */
    public List<List<HbaseCell>> getRow(String tableName, String columnFamily, String rowKey) throws IOException {  
        // 获取表  
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));  
        // 通过rowkey创建一个get对象  
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(columnFamily));
        // 输出结果  
        Result result = table.get(get);  
        List<List<HbaseCell>> rowList = new ArrayList<List<HbaseCell>>();
        List<HbaseCell> cellList = new ArrayList<HbaseCell>();
        for (Cell cell : result.rawCells()) {  
        	HbaseCell hbase = new HbaseCell(new String(CellUtil.cloneRow(cell)),
        			new String(CellUtil.cloneFamily(cell)),
        			new String(CellUtil.cloneQualifier(cell)),
        			Bytes.toString(CellUtil.cloneValue(cell)),
        			cell.getTimestamp());
        	cellList.add(hbase);
        }  
        rowList.add(cellList);
        // 关闭资源  
        table.close();  
        
        return rowList;
    }
    
    /**
     *  
     * @param tableName
     * @param columnFamily
     * @param rowKey
     * @throws IOException
     */
    public List<List<HbaseCell>> getRowByFamilys(String tableName, String[] columnFamilys, String rowKey,String ... longTypeColumns) throws IOException {  
        // 获取表  
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));  
        // 通过rowkey创建一个get对象  
        Get get = new Get(Bytes.toBytes(rowKey));
        if(columnFamilys != null){
        	for(String family : columnFamilys){
        		get.addFamily(Bytes.toBytes(family));
        	}
        }
        // 输出结果  
        Result result = table.get(get);  
        List<List<HbaseCell>> rowList = new ArrayList<List<HbaseCell>>();
        List<HbaseCell> cellList = new ArrayList<HbaseCell>();
        for (Cell cell : result.rawCells()) {  
        	Object value = null;
        	String column = new String(CellUtil.cloneQualifier(cell));
        	//判断是否是long类型
        	if(isLongType(column,longTypeColumns)){
        		value = Bytes.toLong(CellUtil.cloneValue(cell));
        	}else{
        		value = Bytes.toString(CellUtil.cloneValue(cell));
        	}
        	HbaseCell hbase = new HbaseCell(new String(CellUtil.cloneRow(cell)),
        			new String(CellUtil.cloneFamily(cell)),
        			column,
        			value,
        			cell.getTimestamp());
        	cellList.add(hbase);
        }  
        rowList.add(cellList);
        // 关闭资源  
        table.close();  
        
        return rowList;
    }


	
    
    private boolean isLongType(String column,String ... longTypeColumns){
    	if(longTypeColumns == null || longTypeColumns.length == 0){
    		return false;
    	}
		for(String key : longTypeColumns){
			if(key.equalsIgnoreCase(column)){
				return true;
			}
		}
    	return false;
    }
	
    /**
     * 
     * @param tableName
     * @param columnFamily
     * @param rowregex
		 * @return 
     * @throws IOException
     */
    public List<List<HbaseCell>> vagueScanTable(String tableName, String columnFamily,String regex) throws IOException {  
        // 获取表  
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));  
        // 创建一个扫描对象  
		Scan scan = new Scan();
		scan.addFamily(columnFamily.getBytes());
        Filter filter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator(regex));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        List<List<HbaseCell>> rowList = new ArrayList<List<HbaseCell>>();
        for(Result res : scanner){
        	List<HbaseCell> cellList = new ArrayList<HbaseCell>();
        	for (Cell cell : res.rawCells()) {  
             	HbaseCell hbase = new HbaseCell(new String(CellUtil.cloneRow(cell)),
             			new String(CellUtil.cloneFamily(cell)),
             			new String(CellUtil.cloneQualifier(cell)),
             			Bytes.toString(CellUtil.cloneValue(cell)),
             			cell.getTimestamp());
             	cellList.add(hbase);
            } 
        	rowList.add(cellList);
        }
        scanner.close();
        table.close();
        return rowList;
    }
    /**
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param regex
	 * @param currentPage
	 * @param pageSize
	 * @return 
	 * @throws IOException
	 */
    public List<List<HbaseCell>> queryRowCount(String tableName,String columnFamily,String regex,Integer currentPage, Integer pageSize) throws IOException {  
        // 获取表  
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));  
        // 创建一个扫描对象  
        Scan scan = new Scan();
		scan.addFamily(columnFamily.getBytes());
        Filter filter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator(regex));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        List<List<HbaseCell>> rowList = new ArrayList<List<HbaseCell>>();
        int i = 1;
        for (Result r : scanner) {
        	if(i <= (currentPage - 1 ) * pageSize){
        		continue;
        	}
        	if(i > currentPage * pageSize){
        		break;
        	}
        	List<HbaseCell> cellList = new ArrayList<HbaseCell>();
        	for (Cell cell : r.rawCells()) {  
             	HbaseCell hbase = new HbaseCell(new String(CellUtil.cloneRow(cell)),
             			new String(CellUtil.cloneFamily(cell)),
             			new String(CellUtil.cloneQualifier(cell)),
             			Bytes.toString(CellUtil.cloneValue(cell)),
             			cell.getTimestamp());
             	cellList.add(hbase);
            } 
        	i++;
        	rowList.add(cellList);
        }
        // 关闭资源  
        scanner.close();  
        table.close();  
        
        return rowList;
    }


	/**
	 * @param tableName
	 * @param columnFamily
	 * @param prefixs
	 * @return
	 * @throws IOException
	 */
	public Map<String,List<HbaseCell>>  prefixFilters(String tableName, String columnFamily,ArrayList<String> prefixs) throws IOException {
		// 获取表
		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
		FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);

		for(String prefix:prefixs){
			Filter filter = new PrefixFilter(Bytes.toBytes(prefix));
			allFilters.addFilter(filter);
		}

		// 创建一个扫描对象
		Scan scan = new Scan();
		scan.addFamily(columnFamily.getBytes());

		scan.setFilter(allFilters);

		ResultScanner scanner = table.getScanner(scan);
		Map<String,List<HbaseCell>>  data = new HashMap<>();
		for(Result res : scanner){
			List<HbaseCell> cellList = new ArrayList<>();
			for (Cell cell : res.rawCells()) {
				HbaseCell hbase = new HbaseCell(new String(CellUtil.cloneRow(cell)),
						new String(CellUtil.cloneFamily(cell)),
						new String(CellUtil.cloneQualifier(cell)),
						Bytes.toString(CellUtil.cloneValue(cell)),
						cell.getTimestamp());
				cellList.add(hbase);
			}
			data.put(new String(res.getRow()),cellList);
		}

		scanner.close();
		table.close();
		return data;
	}


	/**
	 * @param tableName
	 * @param columnFamily
	 * @param prefix
	 * @return
	 * @throws IOException
	 */
	public List<List<HbaseCell>> prefixFilter(String tableName, String columnFamily,String prefix) throws IOException {
        // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        // 创建一个扫描对象
		Scan scan = new Scan();
		scan.addFamily(columnFamily.getBytes());
        Filter filter = new PrefixFilter(Bytes.toBytes(prefix));
        scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);

        List<List<HbaseCell>> rowList = new ArrayList<List<HbaseCell>>();
        for(Result res : scanner){
        	List<HbaseCell> cellList = new ArrayList<HbaseCell>();
        	for (Cell cell : res.rawCells()) {
             	HbaseCell hbase = new HbaseCell(new String(CellUtil.cloneRow(cell)),
             			new String(CellUtil.cloneFamily(cell)),
             			new String(CellUtil.cloneQualifier(cell)),
             			Bytes.toString(CellUtil.cloneValue(cell)),
             			cell.getTimestamp());
             	cellList.add(hbase);
            }
        	rowList.add(cellList);
        }
        scanner.close();
        table.close();
        return rowList;
    }
    
    /**
     * 
     * @param tableName
     * @param columnFamily
     * @param rowregex
		 * @return 
     * @throws IOException
     */
    public List<List<HbaseCell>> queryLimit(String tableName, String columnFamily,String startKey,String endKey) throws IOException {  
        // 获取表  
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));  
        // 创建一个扫描对象  
		Scan scan = new Scan();
		scan.addFamily(columnFamily.getBytes());
		scan.setStartRow(Bytes.toBytes(startKey)).setStopRow(Bytes.toBytes(endKey));
        ResultScanner scanner = table.getScanner(scan);
        List<List<HbaseCell>> rowList = new ArrayList<List<HbaseCell>>();
        for(Result res : scanner){
        	List<HbaseCell> cellList = new ArrayList<HbaseCell>();
        	for (Cell cell : res.rawCells()) {  
             	HbaseCell hbase = new HbaseCell(new String(CellUtil.cloneRow(cell)),
             			new String(CellUtil.cloneFamily(cell)),
             			new String(CellUtil.cloneQualifier(cell)),
             			Bytes.toString(CellUtil.cloneValue(cell)),
             			cell.getTimestamp());
             	cellList.add(hbase);
            } 
        	rowList.add(cellList);
        }
        scanner.close();
        table.close();
        return rowList;
    }


	/**
	 * @param tableName
	 * @param columnFamily
	 * @param limit
	 * @return
	 * @throws IOException
	 */
	public List<List<HbaseCell>> queryLimit(String tableName, String columnFamily,long limit) throws IOException {
		// 获取表
		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
		// 创建一个扫描对象
		Scan scan = new Scan();
		scan.addFamily(columnFamily.getBytes());
		scan.setFilter(new PageFilter(limit));
		ResultScanner scanner = table.getScanner(scan);
		List<List<HbaseCell>> rowList = new ArrayList<List<HbaseCell>>();
		for(Result res : scanner){
			List<HbaseCell> cellList = new ArrayList<HbaseCell>();
			for (Cell cell : res.rawCells()) {
				HbaseCell hbase = new HbaseCell(new String(CellUtil.cloneRow(cell)),
						new String(CellUtil.cloneFamily(cell)),
						new String(CellUtil.cloneQualifier(cell)),
						Bytes.toString(CellUtil.cloneValue(cell)),
						cell.getTimestamp());
				cellList.add(hbase);
			}
			rowList.add(cellList);
		}
		scanner.close();
		table.close();
		return rowList;
	}

	/**
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public List<String> getRowkeys(String tableName) throws IOException{
    	// 获取表  
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));  
        // 创建一个扫描对象  
		Scan scan = new Scan();
		
		ResultScanner scanner = table.getScanner(scan);
		List<String> list = new ArrayList<String>();
		for (Result r: scanner) { 
			byte[] key = r.getRow();
			list.add(new String(key));
		}
		return list;
    }
    
	public static void main(String[] args) {
    	try {
//    		HBaseUtil.getInstance().createTable("sp_test", new String[] {"info","kpi"});
//    		String[] cfs= {"info"};
			//HBaseUtil.getInstance().createTable("sp_test",cfs);




    		//HBaseUtil.setZookeeper("bigdata-platform-test-01,bigdata-platform-test-02,bigdata-platform-test-03");
			HBaseUtil.setZookeeper("10.254.1.153,10.254.0.81");

			JSONObject values = new JSONObject();
    		for(int i = 0;i < 10;i++) {
    			values.put("key_" + i, "value_" + i);
    		}
    		
    		//HBaseUtil.getInstance().insertData("sp_test", "emp_s", "info", values);

    		HBaseUtil.getInstance().prefixFilter("t_trade","info","78982611_78982611_100189_1006565484");

//    		long start = System.currentTimeMillis();
////    		System.out.println(System.currentTimeMillis() - start);
//    		List<List<HbaseCell>> list = HBaseUtil.getInstance().getRow("sp_test", "info", "emp_s");
//    		System.out.println(System.currentTimeMillis() - start);
//    		list.forEach(celllist -> {
//    			celllist.forEach(cell -> {
//    				System.out.println(cell.getRowkey()+","+ cell.getFamily() + "," + cell.getColumn() + "," + cell.getTimestamp()+","+cell.getValue());
//    			});
//    		});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

