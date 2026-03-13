package com.spatial.util

import org.apache.spark.sql.SparkSession

// 生产任务：将原始 CSV 清洗并转化为 Parquet
object DataPreprocessor {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: DataPreprocessor <input_csv_path> <output_parquet_path>")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("TrajectoryDataPreprocessor")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    // 读取原始 CSV (假设包含 id, lon, lat, timestamp)
    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    // 类型转换与清洗：确保数据格式完全符合匹配引擎的要求
    val cleanedDf = rawDf
      .selectExpr("id", "cast(lon as double)", "cast(lat as double)", "cast(timestamp as long)")
      .filter("lat is not null and lon is not null")

    // 写入 Parquet，按时间戳分区，优化后续的 Join 查询
    cleanedDf.write
      .mode("overwrite")
      .partitionBy("timestamp") // 进阶优化：按时间分区
      .parquet(args(1))

    spark.stop()
  }
}