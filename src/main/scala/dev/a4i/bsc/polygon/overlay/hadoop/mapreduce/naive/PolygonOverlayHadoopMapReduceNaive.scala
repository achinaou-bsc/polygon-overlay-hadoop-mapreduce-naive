package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Options
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.live.PolygonOverlayNaiveMapperLive
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.live.PolygonOverlayNaiveReducerLive
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model.TaggedGeometryWritable

class PolygonOverlayHadoopMapReduceNaive extends Configured, Tool:

  private val jobType: String          = "polygon-overlay"
  private val jobTypeQualifier: String = "hadoop-mapreduce-naive"

  var jobId: Option[String] = None

  override def run(args: Array[String]): Int =
    val options: Options = Options()
      .addRequiredOption(/* opt */ null, "base", /* hasArg */ true, "Base layer GeoJSON")
      .addRequiredOption(/* opt */ null, "overlay", /* hasArg */ true, "Overlay layer GeoJSON")
      .addRequiredOption(/* opt */ null, "output", /* hasArg */ true, "Output directory")
      .addRequiredOption(/* opt */ null, "reference-id", /* hasArg */ true, "Run identifier")
      .addOption(/* opt */ null, "wait-for-completion", /* hasArg */ true, "Wait for the completion of the job")

    val commandLine: CommandLine = DefaultParser().parse(options, args, /* stopAtNonOption = */ false)

    val base: Path                 = Path(commandLine.getOptionValue("base"))
    val overlay: Path              = Path(commandLine.getOptionValue("overlay"))
    val output: Path               = Path(commandLine.getOptionValue("output"))
    val referenceId: String        = commandLine.getOptionValue("reference-id")
    val waitForCompletion: Boolean = commandLine.getOptionValue("wait-for-completion", "true").toBoolean

    val configuration: Configuration = getConf
    configuration.set("baseLayer.path", base.toString)
    configuration.set("overlayLayer.path", overlay.toString)

    val jobName: String = s"${jobType}_${jobTypeQualifier}_${referenceId}"

    val job: Job = Job.getInstance(configuration, jobName)

    job.setJarByClass(classOf[PolygonOverlayHadoopMapReduceNaive])

    job.setMapperClass(classOf[PolygonOverlayNaiveMapperLive])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[TaggedGeometryWritable])

    job.setReducerClass(classOf[PolygonOverlayNaiveReducerLive])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job, base)
    FileInputFormat.addInputPath(job, overlay)
    FileOutputFormat.setOutputPath(job, output)

    if waitForCompletion
    then if job.waitForCompletion(true) then 0 else 1
    else
      job.submit
      jobId = Some(job.getJobID.getJtIdentifier)
      0

object PolygonOverlayHadoopMapReduceNaive:

  def main(args: Array[String]): Unit =
    sys.exit(ToolRunner.run(PolygonOverlayHadoopMapReduceNaive(), args))
