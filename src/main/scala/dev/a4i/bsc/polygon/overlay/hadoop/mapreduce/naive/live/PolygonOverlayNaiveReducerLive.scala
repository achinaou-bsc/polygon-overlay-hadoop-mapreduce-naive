package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.live

import java.lang.Iterable as JavaIterable
import scala.jdk.CollectionConverters.given

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.locationtech.jts.geom.Geometry

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model.Counter
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model.LayerType
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model.TaggedGeometry
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model.TaggedGeometryWritable
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.util.GeoJSON

class PolygonOverlayNaiveReducerLive extends PolygonOverlayNaiveReducer:

  override def reduce(
      key: Text,
      values: JavaIterable[TaggedGeometryWritable],
      context: PolygonOverlayNaiveReducer#Context
  ): Unit =
    given PolygonOverlayNaiveReducer#Context = context

    val taggedGeometries: Array[TaggedGeometry] = values.asScala
      .map(_.taggedGeometry)
      .toArray

    val (baseLayerGeometries: Array[Geometry], overlayLayerGeometries: Array[Geometry]) =
      taggedGeometries.partitionMap:
        case TaggedGeometry(LayerType.Base, geometry)    => Left(geometry)
        case TaggedGeometry(LayerType.Overlay, geometry) => Right(geometry)

    val baseLayerGeometry: Geometry = baseLayerGeometries.head

    overlayLayerGeometries.iterator
      .filter(overlaps(baseLayerGeometry))
      .map(overlay(baseLayerGeometry))
      .foreach: overlayGeometry =>
        context.write(NullWritable.get, Text(GeoJSON.serialize(overlayGeometry)))
        context.getCounter(Counter.REDUCE_OUTPUTS_WRITTEN).increment(1)

  private def overlaps(a: Geometry)(b: Geometry)(using context: PolygonOverlayNaiveReducer#Context): Boolean =
    val result: Boolean = a.intersects(b)
    context.getCounter(Counter.INTERSECTIONS_CHECKED).increment(1)
    result

  private def overlay(a: Geometry)(b: Geometry)(using context: PolygonOverlayNaiveReducer#Context): Geometry =
    val result: Geometry = a.intersection(b)
    context.getCounter(Counter.INTERSECTIONS_CALCULATED).increment(1)
    result
