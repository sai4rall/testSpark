package com.sai.test.config;

import org.apache.spark.sql.SparkSession;

public class ClusterConfig {
public static SparkSession getSparkSession(){
return SparkSession.builder().appName("Udmf-data-migration")
// .master("local[1]")
.getOrCreate();

}
}