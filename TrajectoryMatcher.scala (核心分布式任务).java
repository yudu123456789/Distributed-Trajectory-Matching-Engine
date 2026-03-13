package com.spatial

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.locationtech.jts.io.WKTReader
import scala.collection.mutable.ListBuffer

case class Trajectory(id: String, lon: Double, lat: Double, timestamp: Long)
case class Geofence(fenceId: String, wktPolygon: String)
case class MatchResult(trajectoryId: String, fenceId: String, timestamp: Long)

object TrajectoryMatcher {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: TrajectoryMatcher <trajectory_path> <fence_path> <output_path>")
      System.exit(1)
    }

    val trajectoryPath = args(0)
    val fencePath = args(1)
    val outputPath = args(2)

    val spark = SparkSession.builder()
      .appName("TensOfMillions-Spatial-Trajectory-Matcher")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.broadcast.compress", "true")
      .getOrCreate()

    import spark.implicits._

    val fenceDf = spark.read.parquet(fencePath).as[Geofence]

    val localFenceMap: Map[String, List[Geofence]] = fenceDf.collect()
      .flatMap { fence =>
        GeoUtils.getCoveringGeoHashes(fence.wktPolygon).map(geoHash => (geoHash, fence))
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2).toList)
      
    val broadcastFenceMap = spark.sparkContext.broadcast(localFenceMap)

    val trajectoryDf = spark.read.parquet(trajectoryPath).as[Trajectory]

    val matchedResultDf = trajectoryDf.mapPartitions { iterator =>
      val localMap = broadcastFenceMap.value
      val wktReader = new WKTReader() 
      val results = ListBuffer[MatchResult]()

      while (iterator.hasNext) {
        val traj = iterator.next()
        val targetHash = GeoUtils.encodeGeoHash(traj.lat, traj.lon)

        localMap.get(targetHash) match {
          case Some(candidateFences) =>
            candidateFences.foreach { fence =>
              if (GeoUtils.contains(fence.wktPolygon, traj.lat, traj.lon, wktReader)) {
                results += MatchResult(traj.id, fence.fenceId, traj.timestamp)
              }
            }
          case None => 
        }
      }
      results.iterator
    }

    matchedResultDf.toDF()
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)

    spark.stop()
  }
}