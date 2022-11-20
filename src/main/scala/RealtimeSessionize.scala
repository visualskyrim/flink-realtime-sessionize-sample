import java.time.Duration
import java.util.Properties

import com.typesafe.config.ConfigFactory
import config.{FlinkConfig, KafkaConfig}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import processes.sessionize.SessionizeGroupMapFunction
import processes.{Parse, Sessionize}
import schema.{Parsed, Sessionized}
import scopt.OptionParser
import org.json4s.native.Serialization.write
import org.apache.flink.streaming.api.scala._

object RealtimeSessionize {

  val TIMESTAMP_BASELINE = DateTime.parse("2015-07-22T09:00:28.019143Z").getMillis


  object SourceType {
    val KAFKA = "kafka"
    val LOCAL_FS = "local"
  }

  object SinkType {
    val LOCAL_FS = "local"
    //val HDFS = "hdfs"
    //val ES = "es"
    val KAFKA = "kafka"
  }

  private val LOG: Logger = Logger.getLogger(this.getClass)

  case class RealtimeSessionizeOptions(config: String = null, source: String = "local", sink: String = "local",
                                       path: String = null, debug: Boolean = false)

  object RealtimeSessionizeOptions {

    def parser: OptionParser[RealtimeSessionizeOptions] =
      new OptionParser[RealtimeSessionizeOptions]("Realtime Sessionize") {

        opt[String]("config").required().action((arg, option) => option.copy(config = arg))
          .text("The path for config file.")

        opt[String]("source").action((arg, option) => arg match {

          case SourceType.LOCAL_FS => option.copy(source = SourceType.LOCAL_FS)
          case SourceType.KAFKA => option.copy(source = SourceType.KAFKA)
          case _ => throw new Exception(s"Wrong source type: $arg")

        })

        opt[String]("sink").action((arg, option) => arg match {
          case SinkType.LOCAL_FS => option.copy(sink = SinkType.LOCAL_FS)
          case SinkType.KAFKA => option.copy(sink = SinkType.KAFKA)
          case _ => throw new Exception(s"Wrong sink type: $arg")
        })

        opt[String]("path").action((arg, option) => Option(arg) match {
          case Some(p) =>
            if (option.source == SourceType.LOCAL_FS) {
              option.copy(path = p)
            } else {
              throw new Exception("Passing path while source type is not `local`.")
            }
        })

        opt[Unit]("debug")
          .hidden()
          .action((_, option) => option.copy(debug = true))
      }
  }

  def main(args: Array[String]): Unit = {
    RealtimeSessionizeOptions.parser.parse(args, RealtimeSessionizeOptions()) match {
      case None => throw new RuntimeException("Can't parse the parameters")
      case Some(RealtimeSessionizeOptions(propFileName, source, output, path, debug)) =>

        val config = ConfigFactory.load(propFileName)
        val kafkaProps = KafkaConfig.fromRootProps(config)
        val env = FlinkConfig.setupEnv(config)

        implicit val intputTypeInfo = TypeInformation.of(classOf[String])
        val inputStream: DataStream[String] = source match {

          case SourceType.KAFKA => env
            .addSource(
              new FlinkKafkaConsumer[String](
                config.getString("kafka.topic.in"),
                new SimpleStringSchema,
                kafkaProps)
                .setStartFromLatest())
            .uid("khone-data-enrichment")
            .name("inputStream")

          case SourceType.LOCAL_FS =>
            val filePath = if (path.startsWith("file:///")) {
              path
            } else {
              s"file://$path"
            }

            env.readTextFile(filePath)

        }

        // used for debug to create recent ts using the timestamp string in the original log
        val timestampOffset = DateTime.now().getMillis - TIMESTAMP_BASELINE

        implicit val parsedTypeInfo = TypeInformation.of(classOf[Parsed])
        val parsedStream: DataStream[Parsed] = inputStream
          .flatMap(x => Parse.parse(x))
          .uid("parsing")
          .name("ParsedEvent")

        val withTsStream: DataStream[Parsed] = if (debug) {
          parsedStream.map(x => Parse.fakeTs(x, timestampOffset))
        } else {
          parsedStream
        }

        val orderdStream = withTsStream
          .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(60))
            .withTimestampAssigner(new SerializableTimestampAssigner[Parsed] {
              override def extractTimestamp(record: Parsed, recordTimestamp: Long): Long = record.ts
            }))
          .uid("ordering")
          .name("OrderedEvent")

        implicit val sessionizedTypeInfo = TypeInformation.of(classOf[Sessionized])
        implicit val keyedByTypeInfo = TypeInformation.of(classOf[Int])
        val sessionized: DataStream[Sessionized] = orderdStream
          .keyBy(x => Sessionize.getSessionizeKey(x.ip))
          .map(new SessionizeGroupMapFunction())
          .uid("sessionzing")
          .name("SessionizedEvent")


        implicit val formats: DefaultFormats.type = DefaultFormats

        output match {
          case SinkType.LOCAL_FS =>
            // debug time delay
            val outputFilePath = if (path.startsWith("file:///")) {
              s"$path.out"
            } else {
              s"file://$path.out"
            }

            sessionized.map(s => write(s))
              .writeAsText(outputFilePath)

          case SinkType.KAFKA =>
            val properties = new Properties
            properties.setProperty("bootstrap.servers", "")

            val myProducer: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](
              config.getString("kafka.topic.out"),
              new SimpleStringSchema(),
              properties,
              null,
              Semantic.EXACTLY_ONCE,
              FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE
            )

            sessionized.map(x => write(x)).addSink(myProducer)

        }

        env.execute(FlinkConfig.getJobName(config))
    }
  }
}