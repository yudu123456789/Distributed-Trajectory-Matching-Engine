package com.spatial.util

import org.apache.spark.sql.SparkSession
import scala.util.Random

// 模拟生成轨迹数据和地理围栏数据
object DataGenerator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataGenerator").getOrCreate()
    import spark.implicits._

    // 1. 生成 5000 万条轨迹数据
    val trajCount = 50000000
    val trajData = (1 to trajCount).map { i =>
      (s"traj_$i", 116.0 + Random.nextDouble(), 39.0 + Random.nextDouble(), System.currentTimeMillis())
    }.toDF("id", "lon", "lat", "timestamp")
    
    trajData.repartition(200).write.mode("overwrite").parquet("hdfs:///data/trajectory/daily")

    // 2. 生成 50 万个地理围栏 (WKT格式)
    val fenceCount = 500000
    val fenceData = (1 to fenceCount).map { i =>
      val wkt = s"POLYGON((116.0 39.0, 116.1 39.0, 116.1 39.1, 116.0 39.1, 116.0 39.0))"
      (s"fence_$i", wkt)
    }.toDF("fenceId", "wktPolygon")

    fenceData.repartition(10).write.mode("overwrite").parquet("hdfs:///data/geofence/high_risk")
    
    spark.stop()
  }
}