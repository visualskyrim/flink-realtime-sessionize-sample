package config

import java.util.Properties

import com.typesafe.config.Config

object KafkaConfig {
  private val IN_TOPIC_CONFIG = "kafka.topic.in"
  private val out_TOPIC_CONFIG = "kafka.topic.out"
  private val BOOTSTRAP_SERVERS_CONFIG = "kafka.bootstrap.servers"
  private val ZOOKEEPER_CONNECT_CONFIG = "kafka.zookeeper.connect"
  private val GROUP_ID_CONFIG = "kafka.group.id"


  // Kafka config keys
  private val KAFKA_TOPIC_CONFIG = "topic"
  private val KAFKA_BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers"
  private val KAFKA_ZOOKEEPER_CONNECT_CONFIG = "zookeeper.connect"
  private val KAFKA_GROUP_ID_CONFIG = "group.id"

  private val SECURITY_PROTOCOL = "security.protocol"
  private val KAFKA_SECURITY_PROTOCOL = "kafka.security.protocol"
  private val SASL_MECHANISM = "sasl.mechanism"
  private val KAFKA_SASL_MECHANISM = "kafka.sasl.mechanism"
  private val SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name"
  private val KAFKA_SASL_KERBEROS_SERVICE_NAME = "kafka.sasl.kerberos.service.name"


  def fromRootProps(props: Config): Properties = {
    val kafkaProps = new Properties()
    kafkaProps.setProperty(KAFKA_BOOTSTRAP_SERVERS_CONFIG, props.getString(BOOTSTRAP_SERVERS_CONFIG))
    if (props.hasPath(ZOOKEEPER_CONNECT_CONFIG)) kafkaProps.setProperty(KAFKA_ZOOKEEPER_CONNECT_CONFIG, props.getString(ZOOKEEPER_CONNECT_CONFIG))
    kafkaProps.setProperty(KAFKA_GROUP_ID_CONFIG, props.getString(GROUP_ID_CONFIG))
    if (props.hasPath(KAFKA_SECURITY_PROTOCOL)) {
      kafkaProps.setProperty(SECURITY_PROTOCOL, props.getString(KAFKA_SECURITY_PROTOCOL))
      kafkaProps.setProperty(SASL_MECHANISM, props.getString(KAFKA_SASL_MECHANISM))
      kafkaProps.setProperty(SASL_KERBEROS_SERVICE_NAME, props.getString(KAFKA_SASL_KERBEROS_SERVICE_NAME))
    }
    kafkaProps
  }

  def getInTopic(kafkaConfig: Properties): String = kafkaConfig.getProperty(KAFKA_TOPIC_CONFIG)

  def isSecure(kafkaConfig: Properties): Boolean = kafkaConfig.containsKey(SECURITY_PROTOCOL)
}
