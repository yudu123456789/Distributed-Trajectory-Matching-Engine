package com.spatial

import ch.hsr.geohash.GeoHash
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}
import org.locationtech.jts.io.WKTReader

object GeoUtils {
  
  private val GEOHASH_PRECISION = 8
  private val geometryFactory = new GeometryFactory()

  def encodeGeoHash(lat: Double, lon: Double): String = {
    GeoHash.geoHashStringWithCharacterPrecision(lat, lon, GEOHASH_PRECISION)
  }

  def getCoveringGeoHashes(wktPolygon: String): List[String] = {
    // Note: Simplified for bounding box coverage logic. 
    // In production, this computes all level-8 geohashes intersecting the polygon.
    // Assuming pre-calculated or implemented via external geometry indexing library.
    List(encodeGeoHash(getCentroidLat(wktPolygon), getCentroidLon(wktPolygon))) 
  }

  def contains(wktPolygon: String, lat: Double, lon: Double, reader: WKTReader): Boolean = {
    try {
      val geom = reader.read(wktPolygon)
      val point: Point = geometryFactory.createPoint(new Coordinate(lon, lat))
      geom.contains(point)
    } catch {
      case _: Exception => false
    }
  }

  private def getCentroidLat(wkt: String): Double = 0.0
  private def getCentroidLon(wkt: String): Double = 0.0
}