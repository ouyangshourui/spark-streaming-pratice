package com.whz.platform.sparkKudu;

import com.google.common.collect.ImmutableList;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.*;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author peiyu
 */
public class Kudu2HiveSparkDemo {


    private static final List<String> QUERY_COLUMN_LIST = ImmutableList.of("", "", "", "", "", "");


    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("kudu2HiveDemo").setMaster("local");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        KuduContext kuduContext = new KuduContext("172.172.241.228:7051", javaSparkContext.sc());

        KuduClient client = kuduContext.syncClient();
        KuduTable kuduTable = client.openTable("event_6.event_6_tracker_kudu_40h_2r_de6666");
        KuduScanner.KuduScannerBuilder scannerBuilder = client.newScannerBuilder(kuduTable);

        //设置需要查询的列
        scannerBuilder.setProjectedColumnNames(QUERY_COLUMN_LIST);

        //设置查询的条件，归档的条件是时间
//        ColumnSchema dt = kuduTable.getSchema().getColumn("dt");
//        scannerBuilder.addPredicate(KuduPredicate.newComparisonPredicate(dt, KuduPredicate.ComparisonOp.GREATER_EQUAL, ""));
//        scannerBuilder.addPredicate(KuduPredicate.newComparisonPredicate(dt, KuduPredicate.ComparisonOp.LESS, ""));
        KuduScanner scanner = scannerBuilder.build();

        List<ColumnSchema> columns = scanner.getProjectionSchema().getColumns();

        List<Object[]> list = new ArrayList<>();

        while (scanner.hasMoreRows()) {
            RowResultIterator rowResults = scanner.nextRows();
            for (RowResult result : rowResults) {
                Object[] os = new Object[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    os[i] = parse(result, columns.get(i));
                }
                list.add(os);
            }
        }


        SparkSession sparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate();


        List<StructField> structFields = new ArrayList<>();

        for (ColumnSchema schema : columns) {
            structFields.add(parse(schema));
        }

        StructType structType = DataTypes.createStructType(structFields);


        JavaRDD<Row> javaRDD = javaSparkContext.parallelize(list).map(RowFactory::create);

        Dataset<Row> dataset = sparkSession.createDataFrame(javaRDD, structType);

        dataset.createOrReplaceTempView("temp_event_6_tracker_kudu_40h_2r_de6666");

        sparkSession.sql("insert overwrite table fdm.fdm_tracker_detail_da66666 select * from temp_event_6_tracker_kudu_40h_2r_de6666");

    }

    private static StructField parse(ColumnSchema column) {
        switch (column.getType()) {
            case BOOL:
                return DataTypes.createStructField(column.getName(), DataTypes.BooleanType, true);
            case INT8:
                return DataTypes.createStructField(column.getName(), DataTypes.ByteType, true);
            case INT16:
                return DataTypes.createStructField(column.getName(), DataTypes.ShortType, true);
            case INT32:
                return DataTypes.createStructField(column.getName(), DataTypes.IntegerType, true);
            case INT64:
                return DataTypes.createStructField(column.getName(), DataTypes.LongType, true);
            case FLOAT:
                return DataTypes.createStructField(column.getName(), DataTypes.FloatType, true);
            case DOUBLE:
                return DataTypes.createStructField(column.getName(), DataTypes.DoubleType, true);
            case STRING:
                return DataTypes.createStructField(column.getName(), DataTypes.StringType, true);
            case BINARY:
                return DataTypes.createStructField(column.getName(), DataTypes.BinaryType, true);
            case UNIXTIME_MICROS:
                return DataTypes.createStructField(column.getName(), DataTypes.LongType, true);
            default:
                return DataTypes.createStructField(column.getName(), DataTypes.StringType, true);
        }
    }

    private static Object parse(RowResult result, ColumnSchema column) {
        switch (column.getType()) {
            case BOOL:
                return result.isNull(column.getName()) ? null : result.getBoolean(column.getName());
            case INT8:
                return result.isNull(column.getName()) ? null : result.getByte(column.getName());
            case INT16:
                return result.isNull(column.getName()) ? null : result.getShort(column.getName());
            case INT32:
                return result.isNull(column.getName()) ? null : result.getInt(column.getName());
            case INT64:
                return result.isNull(column.getName()) ? null : result.getLong(column.getName());
            case FLOAT:
                return result.isNull(column.getName()) ? null : result.getFloat(column.getName());
            case DOUBLE:
                return result.isNull(column.getName()) ? null : result.getDouble(column.getName());
            case STRING:
                return result.isNull(column.getName()) ? null : result.getString(column.getName());
            case BINARY:
                return result.isNull(column.getName()) ? null : result.getBinary(column.getName());
            case UNIXTIME_MICROS:
                return result.isNull(column.getName()) ? null : result.getLong(column.getName());
            default:
                return result.isNull(column.getName()) ? null : result.getString(column.getName());
        }
    }

}
