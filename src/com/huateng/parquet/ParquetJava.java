package com.huateng.parquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

import com.huawei.hadoop.security.LoginUtil;

public class ParquetJava {

	public static void main(String[] args) throws Exception {
		String userPrincipal = "sparkuser";
		String userKeytabPath = "/opt/FIclient/user.keytab";
		String krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf";
		Configuration hadoopConf = new Configuration();
		LoginUtil
				.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("parquet");
		
		SparkContext sc = new SparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sc);
		
	}
	
}
