package schema

case class Sessionized(ts: Long, timestamp: String, ip: String, sessionId: String, duration: Int)

object Sessionized {
  def apply(sessionId: String, event: Parsed, duration: Int): Sessionized =
    new Sessionized(event.ts, event.timestamp, event.ip, sessionId, duration)
}
