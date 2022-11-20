package config

import java.util
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

import com.typesafe.config.{Config, ConfigValue}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkConfig {
  private val FLINK_RESTART_ATTEMPTS_CONFIG = "flink.restartAttempts"
  private val FLINK_DELAY_BETWEEN_ATTEMPTS_CONFIG = "flink.delayBetweenAttempts"
  private val FLINK_CHECKPOINT_INTERVAL_CONFIG = "flink.checkpointInterval"
  private val FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS = "flink.minPauseBetweenCheckpoints"
  private val FLINK_CHECKPOINT_TIMEOUT = "flink.checkpointTimeout"
  private val FLINK_MAX_CONCURRENT_CHECKPOINTS = "flink.maxConcurrentCheckpoints"
  private val FLINK_PARALLELISM = "flink.parallelism"
  private val FLINK_MAX_PARALLELISM = "flink.maxParallelism"
  private val FLINK_JOB_NAME = "flink.job.name"

  private val DEFAULT_FLINK_PARALLELISM = 1

  def setupEnv(props: Config): StreamExecutionEnvironment = { // Setup stream environment
    var restartAttempts = 4
    if (props.hasPath(FLINK_RESTART_ATTEMPTS_CONFIG)) restartAttempts = props.getInt(FLINK_RESTART_ATTEMPTS_CONFIG)
    var delayBetweenAttempts = 10000
    if (props.hasPath(FLINK_DELAY_BETWEEN_ATTEMPTS_CONFIG)) delayBetweenAttempts = props.getInt(FLINK_DELAY_BETWEEN_ATTEMPTS_CONFIG)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenAttempts))
    val showConfig = new util.HashMap[String, String]
    props.entrySet.forEach(new Consumer[util.Map.Entry[String, ConfigValue]] {
      override def accept(t: util.Map.Entry[String, ConfigValue]): Unit = showConfig.put(t.getKey, t.getValue.toString)
    })

    env.getConfig.setGlobalJobParameters(ParameterTool.fromMap(showConfig)) // make parameters available in the web interface

    var parallelism = DEFAULT_FLINK_PARALLELISM
    if (props.hasPath(FLINK_PARALLELISM)) parallelism = props.getInt(FLINK_PARALLELISM)
    env.setParallelism(parallelism)
    if (props.hasPath(FLINK_MAX_PARALLELISM)) env.setMaxParallelism(props.getInt(FLINK_MAX_PARALLELISM))
    // Checkpoint config
    var checkpointInterval = 600000
    if (props.hasPath(FLINK_CHECKPOINT_INTERVAL_CONFIG)) checkpointInterval = props.getInt(FLINK_CHECKPOINT_INTERVAL_CONFIG)
    var minPauseBetweenCheckpoints = 300000
    if (props.hasPath(FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS)) minPauseBetweenCheckpoints = props.getInt(FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS)
    var checkpointTimeout = 300000
    if (props.hasPath(FLINK_CHECKPOINT_TIMEOUT)) checkpointTimeout = props.getInt(FLINK_CHECKPOINT_TIMEOUT)
    var maxConcurrentCheckpoints = 1
    if (props.hasPath(FLINK_MAX_CONCURRENT_CHECKPOINTS)) maxConcurrentCheckpoints = props.getInt(FLINK_MAX_CONCURRENT_CHECKPOINTS)
    env.enableCheckpointing(checkpointInterval)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints)
    env.getCheckpointConfig.setCheckpointTimeout(checkpointTimeout)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(maxConcurrentCheckpoints)

    //env.getCheckpointConfig().enableExternalizedCheckpoints(
    //        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


    env.setRestartStrategy(RestartStrategies.failureRateRestart(
      3, // max failures per unit
      time.Time.of(2, TimeUnit.HOURS), //time interval for measuring failure rate
      time.Time.of(4, TimeUnit.MINUTES) // delay
    ))

    env
  }
  def getJobName(props: Config): String = props.getString(FLINK_JOB_NAME)
}
