package com.jyzh;

import  org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HdfsDispose {

	//显示日志帮助调试
	private static final Logger LOGGER = LoggerFactory.getLogger(HdfsDispose.class);

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("transform");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		JavaRDD<String>information = sc.textFile("hdfs://192.168.1.104:8020/user/jiangyuezihan/sample.txt");
		//创建对应编码
		String schemaString = "data_id name gender id rollid citey country catgory islocalprovince school grade district total special_grant tuition";
		//对应的编码添加到格式化字段list中
		List<StructField> fields = new ArrayList<StructField>();
		for(String fieldname:schemaString.split(" ")){
			fields.add(DataTypes.createStructField(fieldname,DataTypes.StringType,true));
		}
		//创建schema类型
		StructType schema = DataTypes.createStructType(fields);
		//数据文件解析并返回RDD格式
		JavaRDD<Row> rowRDD = information.map(new Function<String,Row>(){
			@Override
			public Row call(String record) throws Exception{
				String[] fields = record.split(",");
				return RowFactory.create(fields[0],fields[1],fields[2],fields[3],fields[4],fields[5],fields[6],fields[7],fields[8],fields[9],fields[10],fields[11],fields[12],fields[13],fields[14].trim());
			}});
		//通过指定模式和分割的数据创建dataframe
		Dataset<Row> informationDataSet = sqlContext.createDataFrame(rowRDD, schema);
		informationDataSet.show(10);
		//创建表
		informationDataSet.registerTempTable("people");
		//查询表中数据返回名字
		//Dataset results = sqlContext.sql("select name from people");
		//results.show(10);

//		List<String> names = results.javaRDD().map(new Function<Row,String>() {
//			public String call(Row row) throws Exception {
//				System.out.println(row.toString());
//				System.out.println(row.size());
//				String name = row.getString(0);
//				return "names:" + name;
//			}
//		}).collect();

//		for(String name:names){
//			System.out.println("符合条件的人："+name);
//		}

		//性别转换
		sqlContext.udf().register("gender_exchange",new UDF1<String,String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public String call(String t1) throws Exception{
				LOGGER.info("t1:{}", t1);
				if(t1.contains("男")) {
					return "1";
				} else {
					return "2";
				}
			}
		},DataTypes.StringType);
		sqlContext.sql("select name,  gender_exchange(gender) as gender from people").show(10);

		//学校类型编码
		sqlContext.udf().register("category_exchange",new UDF1<String,String>(){

			public String call(String t2) throws Exception{
				LOGGER.info("t2:{}",t2);
				if(t2.contains("学前教育")||t2.contains("幼儿园")){
					return "01";
				}
				else{
					return "99";
				}
			}
		},DataTypes.StringType);
		sqlContext.sql("select name,category_exchange(category) as category from people").show(10);

		//性别转换
		sqlContext.udf().register("gender_change",new UDF1<String,String>(){

			public String call(String t3) throws Exception{
				LOGGER.info("t3:{}",t3);
				if(t3.contains("男")){
					return "女";
				}
				else{
					return "男";
				}
			}
		},DataTypes.StringType);
		sqlContext.sql("Select name,gender_change(gender) as gender from people").show(10);

		//截取姓名字符串
		sqlContext.udf().register("name_change",new UDF1<String,String>(){

			public String call(String t4) throws Exception{
				LOGGER.info("t4:{}",t4);
				if(t4.length() > 0){
					return t4 = t4.substring(0,t4.length()/2);
				}
				else {
					return t4;
				}
			}
		},DataTypes.StringType);
		sqlContext.sql("Select name_change(name) as name from people").show(10);

		//把区里面的所有“区”换成“市”
		sqlContext.udf().register("country_change",new UDF1<String,String>(){

			public String call(String t5) throws Exception{
				LOGGER.info("t5:{}",t5);
				return t5 = t5.replace("区","市");
			}
		},DataTypes.StringType);
		sqlContext.sql("Select name,country_change(country) as country from people").show(10);

	}

//	private List<String> getColumn(Dataset<Row> row, int i){
//		row.toJavaRDD().map(r -> r.getString(i)).collect();
//	}
}




























