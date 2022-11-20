package processes.sessionize

class SessionState(
                    // For session cutting
                    var sessionId: String,
                    var lastTimestamp: Long,
                    var firstTimestamp: Long,
                    var eventCount: Int,
                  ) {
  def this() {
    this(null, 0, 0, 0)
  }

  def copy(sessionId: String = this.sessionId,
           lastTimestamp: Long = this.lastTimestamp,
           firstTimeStamp: Long = this.firstTimestamp,
           eventCount: Int = this.eventCount): SessionState = {
    new SessionState(sessionId, lastTimestamp, firstTimeStamp, eventCount)
  }

}


object SessionState {
  val SESSION_STATE_NAME = "SESSION_STATE_NAME"
}