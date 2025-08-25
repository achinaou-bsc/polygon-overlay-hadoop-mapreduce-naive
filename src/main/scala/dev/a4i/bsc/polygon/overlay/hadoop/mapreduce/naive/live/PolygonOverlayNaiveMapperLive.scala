package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.live

import scala.compiletime.uninitialized
import scala.io.Source
import scala.util.Using

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.locationtech.jts.geom.Geometry

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model.Counter
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model.LayerType
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model.TaggedGeometry
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model.TaggedGeometryWritable
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.util.GeoJSON

class PolygonOverlayNaiveMapperLive extends PolygonOverlayNaiveMapper:

  private var currentLayerType: LayerType        = uninitialized
  private var baseLayerFeatureIds: Array[String] = uninitialized

  override def setup(context: PolygonOverlayNaiveMapper#Context): Unit =
    val fileSystem: FileSystem     = FileSystem.get(context.getConfiguration)
    val currentLayerFilePath: Path = context.getInputSplit.asInstanceOf[FileSplit].getPath
    val baseLayerFilePath: Path    = fileSystem.makeQualified(Path(context.getConfiguration.get("baseLayer.path")))

    currentLayerType =
      if currentLayerFilePath.equals(baseLayerFilePath)
      then LayerType.Base
      else LayerType.Overlay

    baseLayerFeatureIds = Using
      .Manager: use =>
        use(Source.fromInputStream(use(fileSystem.open(baseLayerFilePath)))).getLines
          .map(GeoJSON.parseFeatureId)
          .toArray
      .get

  override def map(key: LongWritable, value: Text, context: PolygonOverlayNaiveMapper#Context): Unit =
    val (id: String, geometry: Geometry) = GeoJSON.parseFeature(value.toString)

    currentLayerType match
      case LayerType.Base    =>
        context.getCounter(Counter.BASE_POLYGONS_READ).increment(1)
        context.write(Text(id), TaggedGeometryWritable(TaggedGeometry(currentLayerType, geometry)))
        context.getCounter(Counter.MAP_OUTPUTS_WRITTEN).increment(1)
      case LayerType.Overlay =>
        context.getCounter(Counter.OVERLAY_POLYGONS_READ).increment(1)

        baseLayerFeatureIds.foreach: baseLayerFeatureId =>
          context.write(Text(baseLayerFeatureId), TaggedGeometryWritable(TaggedGeometry(currentLayerType, geometry)))
          context.getCounter(Counter.MAP_OUTPUTS_WRITTEN).increment(1)
