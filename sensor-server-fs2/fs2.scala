//> using scala 3.5.0
//> using dep co.fs2::fs2-core:3.11.0
//> using dep co.fs2::fs2-io:3.11.0
//> using dep org.typelevel::cats-effect:3.5.4
//> using dep org.http4s::http4s-ember-client::1.0.0-M41
//> using dep org.http4s::http4s-ember-server::1.0.0-M41
//> using dep org.http4s::http4s-dsl::1.0.0-M41
//> using dep org.http4s::http4s-circe::1.0.0-M41
//> using dep io.circe::circe-generic::0.14.10
//> using dep org.typelevel::log4cats-slf4j::2.7.0

import cats.effect.{IO, Temporal}
import org.http4s.client.{Client => HTTPClient}
import org.http4s.Request
import org.http4s.Method
import org.http4s.Uri
import org.http4s.EntityDecoder
import org.http4s.circe.jsonOf
import concurrent.duration.DurationInt
import fs2.io.file.Flags
import fs2.io.file.Flag
import fs2.io.file.Path
import cats.effect.IOApp
import org.typelevel.log4cats.LoggerFactory
import fs2.concurrent.Channel
import scala.concurrent.duration.FiniteDuration
import org.http4s.HttpRoutes
import cats.kernel.Monoid
import org.http4s.Response
import org.http4s.ember.server.EmberServerBuilder
import com.comcast.ip4s.ipv4
import com.comcast.ip4s.port

case object SendTimeout
case class SensorData(timestamp: Long, reading: Float)

type Route = PartialFunction[Request[IO], IO[Response[IO]]]

def createRoute[T](
    endpoint: String,
    queue: Channel[IO, T],
    queueTimeout: FiniteDuration = 10.seconds
)(using EntityDecoder[IO, T]): Route =
  import org.http4s.dsl.*
  import org.http4s.dsl.io.*
  {
    case req @ POST -> Root / path if path == endpoint =>
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

abstract class SensorA():
  type RawData
  protected given decoder: io.circe.Decoder[RawData]
  protected given userDecoder: EntityDecoder[IO, RawData] = jsonOf[IO, RawData]
  def isValid(raw: RawData): Boolean
  def process(raw: RawData): SensorData

  def getStream(
      queueSize: Int,
      queueTimeout: FiniteDuration
  ): IO[(DataStream, Route)] =
    for
      queue <- Channel.bounded[IO, RawData](queueSize)
      routes = createRoute("sensorA", queue, queueTimeout)
      stream = queue.stream.filter(isValid).map(process)
    yield (stream, routes)

abstract class SensorB:
  type RawData
  protected given decoder: io.circe.Decoder[RawData]
  protected given userDecoder: EntityDecoder[IO, RawData] = jsonOf[IO, RawData]
  def process(raw: RawData): Seq[SensorData]

  def getStream(
      queueSize: Int,
      queueTimeout: FiniteDuration
  ): IO[(DataStream, Route)] =
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
    yield (sA.merge(sB), rA.orElse(rB): Route)
  def movingWindow(size: Int) =
    for
      (s, r) <- combined
      s0 =
        s.scan(List.empty[SensorData]): (xs, x) =>
          if xs.length < size then (x +: xs)
          else x +: xs.init
        .tail
    yield (s0, r)
  def withMovingAverage(size: Int) =
    for
      (s, r) <- movingWindow(size)
      s0 = s.map: xs =>
        (xs.head, xs.map(_.reading).sum / xs.length.toFloat)
    yield (s0, r)
end Process

def program(alertAvg: Float, chunkSize: Int, logFile: Path) =
  given loggerFactory: LoggerFactory[IO] =
    org.typelevel.log4cats.slf4j.Slf4jFactory.create[IO]
  val a: SensorA = ???
  val b: SensorB = ???
  val proc = Process(queueSize = 50, queueTimeout = 10.seconds)(a, b)

  for
    (pipe, routes) <- proc.withMovingAverage(chunkSize)
    pipe0 = pipe
      .evalTap: (data, avg) =>
        if avg > 50.0 then
          IO.println(s"Warning: high average reading $avg at ${data.timestamp}")
        else IO.pure(())
      .chunkN(chunkSize)
      .map(_.last.get)
      .flatMap: (data, _) =>
        fs2.Stream(s"${data.timestamp},${data.reading}\n".getBytes()*)
    _ <- IO.both(
      fs2.io.file.Files.forIO
        .writeAll(logFile, Flags(Flag.Append))(pipe0)
        .compile
        .drain,
      EmberServerBuilder
        .default[IO]
        .withHost(ipv4"0.0.0.0")
        .withPort(port"8080")
        .withHttpApp(HttpRoutes.of[IO](routes).orNotFound)
        .build
        .use(_ => IO.never)
        .as(())
    )
  yield ()

  // fs2.io.file.Files.forIO

class Main extends IOApp.Simple:
  def run =
    val alertAvg: Float = 50.0
    val chunkSize = 20
    val logFile = Path("./log.txt")
    program(alertAvg, chunkSize, logFile)
    // EmberClientBuilder
    //   .default[IO]
    //   .build
    //   .use(program(alertAvg, chunkSize, logFile))
