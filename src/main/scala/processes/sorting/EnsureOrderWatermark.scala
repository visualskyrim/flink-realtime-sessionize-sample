package processes.sorting

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import processes.sorting.EnsureOrderWatermark.WATERMARK_SEC
import schema.Parsed

object EnsureOrderWatermark {

  val WATERMARK_SEC: Long = 60 * 1000

}

class EnsureOrderWatermark() extends BoundedOutOfOrdernessTimestampExtractor[Parsed](Time.milliseconds(WATERMARK_SEC)) {
  override def extractTimestamp(event: Parsed): Long = event.ts
}