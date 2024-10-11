import gears.async.Async
import gears.async.default.given
import gears.async.stream.Stream
import org.eclipse.jetty.server.Response
import sensorwatch.architecture.*

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util as ju
import scala.jdk.CollectionConverters.*
import scala.util.Try

case class SensorData(timestamp: Long, reading: Double)

trait SensorEndpoint(path: String):
  type RawData
  def deser(raw: ju.Map[String, Object]): Option[RawData]

  def getRawStream(http: HttpStreams) = http
    .onPath(path)
    .filterHttp(405, "only POST allowed")(_.request.getMethod() == "POST")
    .mapAsync: handle =>
      val json = handle.request.asJson()
      val jsonObj = deser(json)
      if jsonObj.isDefined then
        handle.response.setStatus(202)
        handle.write(true, ByteBuffer.allocate(0))
        handle.finish()
      else Response.writeError(handle.request, handle.response, handle.callback, 400, "illegal payload")
      jsonObj
    .filter(_.isDefined)
    .map(_.get)
end SensorEndpoint

class SensorA(maxDeviation: Double, minMeasurements: Int) extends SensorEndpoint("/sensorA"):
  class SensorValue(val reading: Double, val deviation: Double)
  class RawData(val timestamp: Long, val measurements: Array[SensorValue])

  @throws
  private def deserValue(measurement: Object): SensorValue =
    measurement match
      case null => null
      case raw: ju.Map[?, ?] =>
        val reading = raw.get("reading").asInstanceOf[java.lang.Double]
        val deviation = raw.get("deviation").asInstanceOf[java.lang.Double]
        SensorValue(reading, deviation)

  def deser(raw: ju.Map[String, Object]): Option[RawData] =
    Try:
      val timestamp = raw.get("timestamp").asInstanceOf[java.lang.Long]
      val measurements = raw.get("measurements").asInstanceOf[ju.ArrayList[Object]]
      val measrOpts = measurements.asScala.view.map(deserValue).toArray
      RawData(timestamp, measrOpts)
    .toOption

  def isValid(raw: RawData): Boolean =
    raw.measurements.count(m => m != null && m.deviation < maxDeviation) >= minMeasurements

  def process(raw: RawData): SensorData =
    val readings = raw.measurements.withFilter(m => m != null && m.deviation < maxDeviation).map(_.reading)
    SensorData(raw.timestamp, readings.sum / readings.size.toDouble)

  def getStream(http: HttpStreams) = getRawStream(http).filter(isValid).map(process)
end SensorA

class SensorB(measurementSpacing: Int) extends SensorEndpoint("/sensorB"):
  class RawData(val timestamp: Long, val measurements: Seq[Double])

  def deser(raw: ju.Map[String, Object]): Option[RawData] =
    Try:
      val timestamp = raw.get("timestamp").asInstanceOf[java.lang.Long]
      val measurements = raw.get("measurements").asInstanceOf[ju.ArrayList[?]]
      RawData(timestamp, measurements.asScala.view.map(_.asInstanceOf[java.lang.Double]: Double).toSeq)
    .toOption

  def process(raw: RawData): Seq[SensorData] =
    raw.measurements.zipWithIndex.map: (measurement, index) =>
      SensorData(raw.timestamp + index * measurementSpacing, measurement)

  def getStream(http: HttpStreams) = getRawStream(http).flatMap(Integer.MAX_VALUE): raw =>
    Stream.fromArray(process(raw).toArray).toPushStream()
end SensorB

class Program(sensorA: SensorA, sensorB: SensorB, avgWindowSize: Int, alertAvg: Double, outFreq: Int, outPath: Path):
  def getStream(http: HttpStreams) =
    val streamA = sensorA.getStream(http)
    val streamB = sensorB.getStream(http)

    (streamA merge streamB)
      .movingWindow(avgWindowSize)
      .map(list => (list.head, list.map(_.reading).sum / avgWindowSize))
      .tap: (data, avg) =>
        if avg > alertAvg then println(s"Warning: high average reading $avg at ${data.timestamp}")
      .chunkN(outFreq)
      .map(_.last)
      .map((data, _) => StandardCharsets.US_ASCII.encode(s"${data.timestamp},${data.reading}\n"))
      .writeToFile(outPath, StandardOpenOption.CREATE)

@main
def test() =
  val http = new HttpStreams(8044)
  val sensorA = new SensorA(3.0, 5)
  val sensorB = new SensorB(10)
  val program = new Program(sensorA, sensorB, 20, 50.0, 20, Paths.get("./log.txt"))
  val stream = program.getStream(http)

  Async.blocking:
    http.drainAll(stream)
