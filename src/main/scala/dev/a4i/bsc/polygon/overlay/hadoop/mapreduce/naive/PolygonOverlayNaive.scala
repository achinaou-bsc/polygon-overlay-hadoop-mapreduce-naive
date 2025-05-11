package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.live.PolygonOverlayNaiveMapperLive
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.live.PolygonOverlayNaiveReducerLive
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model.TaggedGeometryWritable

class PolygonOverlayNaive

object PolygonOverlayNaive:

  private val jobType: String                      = "polygon-overlay"
  private val jobTypeQualifier: String             = "mapreduce-naive"
  private def jobName(referenceId: String): String = Array(jobType, jobTypeQualifier, referenceId).mkString("_")

  def main(args: Array[String]): Unit =
    val Array(referenceId) = args

    sys.exit:
      if job(referenceId).waitForCompletion(true)
      then 0
      else 1

  private def job(referenceId: String) =
    val workingDirectory: Path = Path(s"/jobs/$jobType", referenceId)
    val inputDirectory: Path   = Path(workingDirectory, "input")

    val baseLayer: Path    = Path(inputDirectory, "a.geojson")
    val overlayLayer: Path = Path(inputDirectory, "b.geojson")

    val output: Path = Path(workingDirectory, "output")

    val job: Job = Job.getInstance(configuration(baseLayer), jobName(referenceId))

    FileInputFormat.addInputPath(job, baseLayer)
    FileInputFormat.addInputPath(job, overlayLayer)
    FileOutputFormat.setOutputPath(job, output)

    job.setJarByClass(classOf[PolygonOverlayNaive])

    job.setMapperClass(classOf[PolygonOverlayNaiveMapperLive])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[TaggedGeometryWritable])

    job.setReducerClass(classOf[PolygonOverlayNaiveReducerLive])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job

  private def configuration(baseLayer: Path): Configuration =
    val configuration: Configuration = Configuration()

    configuration.set("baseLayer.path", baseLayer.toString)

    configuration
