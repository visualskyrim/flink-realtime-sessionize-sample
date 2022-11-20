package processes

import schema.Parsed
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object Parse {
  val formatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")

  def parse(input: String): Option[Parsed] = {
    val lineEntries = input.split(" ")
    if (lineEntries.size != 15) {
      None
    } else {
      Some(Parsed(lineEntries(0), formatter.parseDateTime(lineEntries(0)).getMillis, lineEntries(2)))
    }
  }

  def fakeTs(parsed: Parsed, offsetSet: Long): Parsed = {
    parsed.copy(ts = parsed.ts + offsetSet)
  }
}
