package com.whz.platform.sparkKudu.kudu;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.Upsert;

public class KuduUtil {
	private KuduClient client;
	
	private static KuduUtil ku;
	
	public static KuduUtil getKuduInstance(String kuduMaster){
		if(ku == null){
			ku = new KuduUtil(kuduMaster);
		}else{
			if(!ku.client.isStatisticsEnabled()){
				ku = null;
				getKuduInstance(kuduMaster);
			}
		}
		return ku;
	}
	
	private KuduUtil(String kuduMaster){
		super();
		initKuduClient(kuduMaster);
	}

	public Schema getTableColumns(String table) throws Exception{
		return client.openTable(table).getSchema();
	}

	public void close() throws Exception{
		client.close();
	}

	private void initKuduClient(String kuduMaster){
		client = new KuduClient.KuduClientBuilder(kuduMaster).build();
	}
	
	public void updateColumn(String tableName,Map<String,Object> values) throws KuduException {
		KuduSession session =this.client.newSession();

		KuduTable table =this.client.openTable(tableName);

		Upsert upsert =table.newUpsert();
        for(String column : values.keySet()){
        	ColumnSchema queryColumnSchema = table.getSchema().getColumn(column);
        	Type queryColumnType = queryColumnSchema.getType();
        	switch (queryColumnType) {
		        case BOOL:
					try {
						boolean booleanValue = Boolean.parseBoolean(values.get(column).toString());
						upsert.getRow().addBoolean(column, booleanValue);
					} catch (Exception e) {
					}
		        	break;
		        case INT8:
					try {
						byte byteValue = Byte.parseByte(values.get(column).toString());
						upsert.getRow().addByte(column, byteValue);
					} catch (Exception e) {
					}
		        	break;
		        case INT16:
					try {
						short shortValue = Short.parseShort(values.get(column).toString());
						upsert.getRow().addShort(column, shortValue);
					} catch (Exception e) {
					}
		        	break;
		        case INT32:
					try {
						int intValue = Integer.parseInt(values.get(column).toString());
						upsert.getRow().addInt(column, intValue);
					} catch (Exception e) {
					}
		        	break;
		        case INT64:
					try {
						long longValue = Long.parseLong(values.get(column).toString());
						upsert.getRow().addLong(column, longValue);
					} catch (Exception e) {
					}
		        	break;
		        case FLOAT:
					try {
						float floatValue = Float.parseFloat(values.get(column).toString());
						upsert.getRow().addFloat(column, floatValue);
					} catch (Exception e) {
					}
		        	break;
		        case DOUBLE:
					try {
						double doubleValue = Double.parseDouble(values.get(column).toString());
						upsert.getRow().addDouble(column, doubleValue);
					} catch (Exception e) {
					}
		        	break;
		        case STRING:
		        	upsert.getRow().addString(column, values.get(column).toString());
		        	break;
		        case BINARY:
					try {
						byte binaryValue = Byte.parseByte(values.get(column).toString());
						upsert.getRow().addByte(column, binaryValue);
					} catch (Exception e) {
					}
		        	break;
		        case UNIXTIME_MICROS:
					try {
						long timeValue = Long.parseLong(values.get(column).toString());
						upsert.getRow().addLong(column, timeValue);
					} catch (Exception e) {
					}
		        	break;
		        default:
		        	upsert.getRow().addString(column, values.get(column).toString());
		        	break;
        	}
        }

        session.apply(upsert);
		session.close();
	}
	
	public List<Map<String, Object>> scan(String tableName,Map<String,Object> conditions,List<String> columnNames) throws KuduException{
		System.out.println("scan:" + tableName);
		KuduTable table = client.openTable(tableName);
		
		KuduScannerBuilder builder = client.newScannerBuilder(table);
		List<Map<String,Object>> resultList = new ArrayList<>();
		
		for(String queryColumn : conditions.keySet()){
			ColumnSchema queryColumnSchema = table.getSchema().getColumn(queryColumn);
			Type queryColumnType = queryColumnSchema.getType();
			//newComparisonPredicate 在一个整数或时间戳列上创建一个新的比较谓词。
	        KuduPredicate predicate = null;
	        Object queryValue = conditions.get(queryColumn);
			switch (queryColumnType) {
		        case BOOL:
		        	predicate = KuduPredicate.newComparisonPredicate(queryColumnSchema,ComparisonOp.EQUAL, Boolean.parseBoolean(queryValue.toString()));
		        	break;
		        case INT8:
		        	predicate = KuduPredicate.newComparisonPredicate(queryColumnSchema,ComparisonOp.EQUAL, Byte.parseByte(queryValue.toString()));
		        	break;
		        case INT16:
		        	predicate = KuduPredicate.newComparisonPredicate(queryColumnSchema,ComparisonOp.EQUAL, Short.parseShort(queryValue.toString()));
		        	break;
		        case INT32:
		        	predicate = KuduPredicate.newComparisonPredicate(queryColumnSchema,ComparisonOp.EQUAL, Integer.parseInt(queryValue.toString()));
		        	break;
		        case INT64:
		        	predicate = KuduPredicate.newComparisonPredicate(queryColumnSchema,ComparisonOp.EQUAL, Long.parseLong(queryValue.toString()));
		        	break;
		        case FLOAT:
		        	predicate = KuduPredicate.newComparisonPredicate(queryColumnSchema,ComparisonOp.EQUAL, Float.parseFloat(queryValue.toString()));
		        	break;
		        case DOUBLE:
		        	predicate = KuduPredicate.newComparisonPredicate(queryColumnSchema,ComparisonOp.EQUAL, Double.parseDouble(queryValue.toString()));
		        	break;
		        case STRING:
		        	predicate = KuduPredicate.newComparisonPredicate(queryColumnSchema,ComparisonOp.EQUAL, queryValue.toString());
		        	break;
		        case BINARY:
		        	predicate = KuduPredicate.newComparisonPredicate(queryColumnSchema,ComparisonOp.EQUAL, Byte.parseByte(queryValue.toString()));
		        	break;
		        case UNIXTIME_MICROS:
		        	predicate = KuduPredicate.newComparisonPredicate(queryColumnSchema,ComparisonOp.EQUAL, Long.parseLong(queryValue.toString()));
		        	break;
		        default:
		        	predicate = KuduPredicate.newComparisonPredicate(queryColumnSchema,ComparisonOp.EQUAL, queryValue.toString());
		        	break;
		    }
	        builder.addPredicate(predicate);
		}
        
        builder.setProjectedColumnNames(columnNames);
        // 开始扫描
        KuduScanner scaner = builder.build();
        while (scaner.hasMoreRows()) {
            RowResultIterator iterator = scaner.nextRows();
            while (iterator.hasNext()) {
                RowResult result = iterator.next();
                Map<String,Object> map = new HashMap<>();
                for(ColumnSchema schema : result.getSchema().getColumns()){
                	String name = schema.getName();
                	Type type = schema.getType();
                	Object val = null;
                	switch (type) {
	                    case BOOL:
	                    	val = result.getBoolean(name);
	                    	break;
	                    case INT8:
	                    	val = result.getByte(name);
	                    	break;
	                    case INT16:
	                    	val = result.getShort(name);
	                    	break;
	                    case INT32:
	                    	val = result.getInt(name);
	                    	break;
	                    case INT64:
	                    	val = result.getLong(name);
	                    	break;
	                    case FLOAT:
	                    	val = result.getFloat(name);
	                    	break;
	                    case DOUBLE:
	                    	val = result.getDouble(name);
	                    	break;
	                    case STRING:
	                    	val = result.getString(name);
	                    	break;
	                    case BINARY:
	                    	val = result.getBinary(name);
	                    	break;
	                    case UNIXTIME_MICROS:
	                    	val = result.getLong(name);
	                    	break;
	                    default:
	                    	val = result.getString(name);
	                    	break;
	                }
                	
                	map.put(name, val);
                }
                
                resultList.add(map);
            }
        }
        scaner.close();
        
        return resultList;
	}
	
	public static void main(String[] args) throws InterruptedException, KuduException {
		List<String> columnNames =new ArrayList<>();
		columnNames.add("name");
		
		Map<String,Object> params = new HashMap<String,Object>();
        params.put("k_ftenancy_id", "100440");
        params.put("code", 50007);
		List<Map<String, Object>> list = KuduUtil.getKuduInstance("10.253.240.11,10.253.240.16,10.253.240.14").scan("event.event_name_code_dim", params,columnNames);
		
		System.out.println(list);
	}
	
}
