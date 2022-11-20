package processes.sessionize

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import processes.sessionize.SessionizeGroupMapFunction.SESSION_MAX_DURATION
import schema.{Parsed, Sessionized}

class SessionizeGroupMapFunction extends RichMapFunction[Parsed, Sessionized] {

  // cache item: ip -> session state
  private var sessionStates: MapState[String, SessionState] = _

  override def map(event: Parsed): Sessionized = {


    // New session
    if (!this.sessionStates.contains(event.ip) || this.sessionStates.get(event.ip) == null) {

      val newSessionId = createSessionId(event)

      this.sessionStates.put(event.ip, new SessionState(createSessionId(event),
        event.ts / 1000,
        event.ts / 1000,
        1))

      Sessionized(newSessionId, event, 0)

    } else { // On going session

      val existingState = this.sessionStates.get(event.ip)

      // Check timeout
      if (event.ts - existingState.lastTimestamp > SessionizeGroupMapFunction.SESSION_TIMEOUT_SEC) {
        // timeout, create new session for the same ip
        val newSessionId = createSessionId(event)

        // create new session state with the incoming event
        val newState = new SessionState(newSessionId, event.ts / 1000, event.ts / 1000, 1)
        this.sessionStates.put(event.ip, newState)

        Sessionized(newSessionId, event, 0)

      }
      // Check max size
      else if (existingState.eventCount + 1 >= SessionizeGroupMapFunction.SESSION_MAX_SIZE) {
        // exceed max session event count, create new session for the same ip
        val newSessionId = createSessionId(event)

        // create new session state with the incoming event
        val newState = new SessionState(newSessionId, event.ts / 1000, event.ts / 1000, 1)
        this.sessionStates.put(event.ip, newState)

        Sessionized(newSessionId, event, 0)

      }
      // Check max duration
      else if (event.ts / 1000 - existingState.firstTimestamp > SessionizeGroupMapFunction.SESSION_MAX_DURATION) {
        // exceed max duration, create new session
        val newSessionId = createSessionId(event)

        val newState = new SessionState(newSessionId, event.ts / 1000, event.ts / 1000, 1)

        this.sessionStates.put(event.ip, newState)

        Sessionized(newSessionId, event, 0)

      } else {
        // Event is still within the ongoing session

        // In case events gets out of order
        val lastTimestamp =
          if (event.ts / 1000 > existingState.lastTimestamp)
            event.ts / 1000
          else
            existingState.lastTimestamp

        this.sessionStates.put(event.ip, existingState.copy(
          lastTimestamp = lastTimestamp,
          eventCount = existingState.eventCount + 1))

        Sessionized(existingState.sessionId, event, (event.ts / 1000 - existingState.firstTimestamp).toInt)
      }
    }

  }


  // Define TTL
  override def open(parameters: Configuration): Unit
  = {
    val descriptor = new MapStateDescriptor(SessionState.SESSION_STATE_NAME, classOf[String], classOf[SessionState])

    val ttlConfig = StateTtlConfig
      .newBuilder(Time.hours(SESSION_MAX_DURATION / 60 / 60))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // assuming 6000 as sessionizing records/per second; 900 is for 15 minutes
      .cleanupInRocksdbCompactFilter(6000L * 60)
      .build()
    descriptor.enableTimeToLive(ttlConfig)

    sessionStates = getRuntimeContext.getMapState(descriptor)
  }

  private def createSessionId(event: Parsed): String = s"${event.ip}-${event.timestamp}"
}


object SessionizeGroupMapFunction {

  private val SESSION_TIMEOUT_SEC = 30 * 60
  private val SESSION_MAX_DURATION = 12 * 60 * 60
  private val SESSION_MAX_SIZE = 1500

}
