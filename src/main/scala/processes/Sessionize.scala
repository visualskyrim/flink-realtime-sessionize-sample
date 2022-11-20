package processes

object Sessionize {
  // TODO: Need to check if this repartition will cause skew
  def getSessionizeKey(ip: String): Int = ip.hashCode % 1200
}
