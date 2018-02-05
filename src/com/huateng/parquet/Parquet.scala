package com.huateng.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
object Parquet {
  case class Busi(BUSI_ID: String, BUSI_CODE: String, BUSI_NAME: String,BUSI_DESC:String,
            BUSI_LEADER:String,SOURCE_TYPE:String,BUSI_TYPE:String,JOIN_FLAG:String,BAN_PERIOD:String,BEG_DATE:String,END_DATE:String,
            MAN_DEPART:String,EXE_DEPART:String,RISK_POLICY:String,OTHER_LIMIT:String,BUSI_STATE:String,LAST_UPD_DATE:String,LAST_UPD_OPR:String)
 def main(args: Array[String]) {
   /*   val userPrincipal = "ark"
      val userKeytabPath = "/opt/FIclient/user.keytab"
      val krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf"
      val hadoopConf: Configuration  = new Configuration()
      LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);*/
      val sparkConf = new SparkConf().setAppName("FemaleInfo").setMaster("local").set("spark.sql.warehouse.dir", "/spark-warehouse/")
      val sc = new SparkContext(sparkConf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      
      import sqlContext.implicits._
      
      val fileRDD = sc.textFile("F:\\DST_BUSI_INFO.del").map(_.split(",")).map(p => Busi(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(15),p(16),p(17)))
      val busi = fileRDD.toDF().registerTempTable("DST_BUSI_INFO")
      val id = sqlContext.sql("select * from DST_BUSI_INFO")
      id.collect().foreach(println)
      id.write.parquet("F:\\DST_BUSI_INFO.parquet")    
  
      
//    val hadoopdsPath = "hdfs://hacluster/"
//   def convert(sqlContext: SQLContext, filename: String, schema: StructType, tablename: String) {
//      // import text-based table first into a data frame
//      val df = sqlContext.read.schema(schema).option("delimiter", "|").load(filename)
//      // now simply write to a parquet file
//      df.write.parquet("/user/spark/data/parquet/"+tablename)
//    }
//
//        // usage exampe -- a tpc-ds table called catalog_page
//    val schema = StructType(Array(
//            StructField("cp_catalog_page_sk",IntegerType,false),
//            StructField("cp_catalog_page_id",StringType,false),
//            StructField("cp_start_date_sk",IntegerType,true),
//            StructField("cp_end_date_sk", IntegerType,true),
//            StructField("cp_department",StringType,true),
//            StructField("cp_catalog_number",LongType,true),
//            StructField("cp_catalog_page_number",LongType,true),
//            StructField("cp_description",StringType,true),
//            StructField("cp_type",StringType,true)))
//
//          convert(sqlContext,
//            hadoopdsPath+"/catalog_page/*",
//            schema,
//            "catalog_page")
//    
  }

}