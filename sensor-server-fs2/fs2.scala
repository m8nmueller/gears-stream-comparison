//> using scala 3.5.0
//> using dep co.fs2::fs2-core:3.11.0
//> using dep co.fs2::fs2-io:3.11.0
//> using dep org.typelevel::cats-effect:3.5.4
//> using dep org.http4s::http4s-ember-server::1.0.0-M41
//> using dep org.http4s::http4s-dsl::1.0.0-M41
//> using dep org.http4s::http4s-circe::1.0.0-M41
//> using dep io.circe::circe-generic::0.14.10
//> using dep org.typelevel::log4cats-slf4j::2.7.0

import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Temporal
import cats.implicits.toSemigroupKOps
import com.comcast.ip4s.Port
import com.comcast.ip4s.ipv4
import com.comcast.ip4s.port
import fs2.concurrent.Channel
import fs2.io.file.Flag
import fs2.io.file.Flags
import fs2.io.file.Path
import io.circe.generic.semiauto.*
import org.http4s.EntityDecoder
import org.http4s.HttpRoutes
import org.http4s.Request
import org.http4s.Response
import org.http4s.circe.jsonOf
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.log4cats.LoggerFactory

import scala.concurrent.duration.FiniteDuration

import concurrent.duration.DurationInt

case class SensorData(timestamp: Long, reading: Double)

def createRoute[T](
    endpoint: String,
    queue: Channel[IO, T],
    queueTimeout: FiniteDuration
)(using EntityDecoder[IO, T]): HttpRoutes[IO] =
  import org.http4s.dsl.*
  import org.http4s.dsl.io.*
  val Path = endpoint

  HttpRoutes.of { case req @ POST -> Root / Path =>
    for
      item <- req.as[T]
      sendResult <- IO.race(
        queue.send(item),
        Temporal[IO].sleep(queueTimeout)
      )
      res <- sendResult match
        case Left(Right(_)) => Ok("ok!")
        case Right(_)       => TooManyRequests("queue is full")
        case Left(Left(_))  => InternalServerError("queue is closed?")
    yield res
  }

class SensorA(maxDeviation: Double, minMeasurements: Int):
  case class SensorValue(val reading: Double, val deviation: Double)
  case class RawData(val timestamp: Long, val measurements: Array[SensorValue])

  protected given valueDecoder: io.circe.Decoder[SensorValue] = deriveDecoder[SensorValue]
  protected given decoder: io.circe.Decoder[RawData] = deriveDecoder[RawData]
  protected given userDecoder: EntityDecoder[IO, RawData] = jsonOf[IO, RawData]

  def isValid(raw: RawData): Boolean =
    raw.measurements.count(m => m != null && m.deviation < maxDeviation) >= minMeasurements

  def process(raw: RawData): SensorData =
    val readings = raw.measurements.withFilter(m => m != null && m.deviation < maxDeviation).map(_.reading)
    SensorData(raw.timestamp, readings.sum / readings.size.toDouble)

  def getStream(
      queueSize: Int,
      queueTimeout: FiniteDuration
  ): IO[(DataStream, HttpRoutes[IO])] =
    for
      queue <- Channel.bounded[IO, RawData](queueSize)
      routes = createRoute("sensorA", queue, queueTimeout)
      stream = queue.stream.filter(isValid).map(process)
    yield (stream, routes)

class SensorB(measurementSpacing: Int):
  case class RawData(val timestamp: Long, val measurements: Seq[Double])

  protected given decoder: io.circe.Decoder[RawData] = deriveDecoder[RawData]
  protected given userDecoder: EntityDecoder[IO, RawData] = jsonOf[IO, RawData]

  def process(raw: RawData): Seq[SensorData] =
    raw.measurements.zipWithIndex.map: (measurement, index) =>
      SensorData(raw.timestamp + index * measurementSpacing, measurement)

  def getStream(
      queueSize: Int,
      queueTimeout: FiniteDuration
  ): IO[(DataStream, HttpRoutes[IO])] =
    for
      queue <- Channel.bounded[IO, RawData](queueSize)
      routes = createRoute("sensorB", queue, queueTimeout)
      stream = queue.stream.flatMap(item => fs2.Stream(process(item)*))
    yield (stream, routes)

type Stream[T] = fs2.Stream[IO, T]
type DataStream = Stream[SensorData]

class Process(queueSize: Int, queueTimeout: FiniteDuration)(
    a: SensorA,
    b: SensorB
):
  val combined =
    for
      (sA, rA) <- a.getStream(queueSize, queueTimeout)
      (sB, rB) <- b.getStream(queueSize, queueTimeout)
    yield (sA.merge(sB), rA <+> rB)

  def movingWindow(size: Int) =
    for
      (s, r) <- combined
      s0 =
        s.scan(List.empty[SensorData]): (xs, x) =>
          if xs.length < size then (x +: xs)
          else x +: xs.init
        .drop(20) // these are incomplete
    yield (s0, r)
  def withMovingAverage(size: Int) =
    for
      (s, r) <- movingWindow(size)
      s0 = s.map: xs =>
        (xs.head, xs.map(_.reading).sum / xs.length.toDouble)
    yield (s0, r)
end Process

def program(proc: Process, port: Port, avgWindowSize: Int, alertAvg: Double, outFreq: Int, logFile: Path) =
  given loggerFactory: LoggerFactory[IO] =
    org.typelevel.log4cats.slf4j.Slf4jFactory.create[IO]

  for
    (pipe, routes) <- proc.withMovingAverage(avgWindowSize)
    pipe0 = pipe
      .evalTap: (data, avg) =>
        if avg > alertAvg then IO.println(s"Warning: high average reading $avg at ${data.timestamp}")
        else IO.pure(())
      .chunkN(outFreq)
      .map(_.last.get)
      .flatMap: (data, _) =>
        fs2.Stream(s"${data.timestamp},${data.reading}\n".getBytes()*)
    _ <- IO.both(
      fs2.io.file.Files.forIO
        .writeAll(logFile, Flags(Flag.Create, Flag.Append))(pipe0)
        .compile
        .drain,
      EmberServerBuilder
        .default[IO]
        .withHost(ipv4"0.0.0.0")
        .withPort(port)
        .withHttpApp(routes.orNotFound)
        .build
        .use(_ => IO.never)
        .as(())
    )
  yield ()

object Main extends IOApp.Simple:
  def run =
    val a = new SensorA(maxDeviation = 3.0, minMeasurements = 5)
    val b = new SensorB(measurementSpacing = 10)
    val proc = Process(queueSize = 1000, queueTimeout = 10.seconds)(a, b)

    val port = port"8044"
    val avgWindowSize = 20
    val alertAvg: Double = 50.0
    val outFreq = 20
    val logFile = Path("./log.txt")
    program(proc, port, avgWindowSize, alertAvg, outFreq, logFile)
