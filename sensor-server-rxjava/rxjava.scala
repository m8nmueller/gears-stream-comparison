//> using scala 3.5.0
//> using dep io.vertx:vertx-rx-java3:4.5.10
//> using dep io.vertx:vertx-web:4.5.10
//> using dep "com.fasterxml.jackson.module::jackson-module-scala:2.18.0"

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.reactivex.rxjava3.core.Observable
import io.vertx.core.file.OpenOptions
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.rxjava3.core.Vertx
import io.vertx.rxjava3.core.buffer.Buffer
import io.vertx.rxjava3.ext.web.Router
import io.vertx.rxjava3.ext.web.RoutingContext
import io.vertx.rxjava3.ext.web.handler.BodyHandler

import scala.jdk.CollectionConverters.*

trait SensorEndpoint[RawData](path: String, clszz: Class[RawData]):
  def getRaw(router: Router) =
    Observable
      .create[RoutingContext]: subscriber =>
        router.post(path).handler(BodyHandler.create()).handler(e => subscriber.onNext(e))
      .map: ctx =>
        val body = ctx.body().asPojo(clszz)
        ctx.response().setStatusCode(202)
        ctx.response().end()
        body

case class SensorData(timestamp: Long, reading: Double)

object SensorA:
  case class SensorValue(@JsonProperty("reading") val reading: Double, @JsonProperty("deviation") val deviation: Double)
  case class RawData(val timestamp: Long, val measurements: Array[SensorValue])

class SensorA(maxDeviation: Double, minMeasurements: Int) extends SensorEndpoint[SensorA.RawData]("/sensorA", classOf):
  import SensorA.*
  def isValid(raw: RawData): Boolean =
    raw.measurements.count(m => m != null && m.deviation < maxDeviation) >= minMeasurements

  def process(raw: RawData): SensorData =
    val readings = raw.measurements.withFilter(m => m != null && m.deviation < maxDeviation).map(_.reading)
    SensorData(raw.timestamp, readings.sum / readings.size.toDouble)

  def getObservable(router: Router) = getRaw(router).filter(isValid).map(process)

object SensorB:
  case class RawData(val timestamp: Long, val measurements: Seq[Double])

class SensorB(measurementSpacing: Int) extends SensorEndpoint[SensorB.RawData]("/sensorB", classOf):
  import SensorB.*

  def process(raw: RawData): Seq[SensorData] =
    raw.measurements.zipWithIndex.map: (measurement, index) =>
      SensorData(raw.timestamp + index * measurementSpacing, measurement)

  def getObservable(router: Router) = getRaw(router).flatMapIterable(d => process(d).asJava)

@main
def hello() =
  DatabindCodec.mapper().registerModule(DefaultScalaModule)

  val vertx = Vertx.vertx()
  val fs = vertx.fileSystem()
  val server = vertx.createHttpServer()
  val router = Router.router(vertx)

  val sensorA = SensorA(maxDeviation = 3.0, minMeasurements = 5)
  val sensorB = SensorB(measurementSpacing = 10)

  val port = 8044
  val avgWindowSize = 20
  val alertAvg: Double = 50.0
  val outFreq = 20
  val logFile = "./log.txt"

  val file = fs.openBlocking(logFile, new OpenOptions().setCreate(true).setAppend(true))

  Observable
    .merge(sensorA.getObservable(router), sensorB.getObservable(router))
    .buffer(avgWindowSize, 1)
    .map(list => (list.getLast(), list.stream().mapToDouble(_.reading).average().getAsDouble()))
    .doOnNext: (data, avg) =>
      if avg > alertAvg then println(s"Warning: high average reading $avg at ${data.timestamp}")
    .window(outFreq)
    .flatMap(window => window.takeLast(1))
    .map((data, _) => Buffer.buffer(s"${data.timestamp},${data.reading}\n", "US-ASCII"))
    .flatMapCompletable(buffer => file.rxWrite(buffer))
    .subscribe()

  server.requestHandler(router).listen(port)
