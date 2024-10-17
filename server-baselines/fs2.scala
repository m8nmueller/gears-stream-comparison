//> using scala 3.5.0
//> using dep org.http4s::http4s-ember-server::1.0.0-M41
//> using dep org.http4s::http4s-dsl::1.0.0-M41
//> using dep org.typelevel::log4cats-slf4j::2.7.0

import cats.effect.IO
import cats.effect.IOApp
import com.comcast.ip4s.ipv4
import com.comcast.ip4s.port
import org.http4s.HttpRoutes
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory

import org.http4s.dsl.*
import org.http4s.dsl.io.*

object Main extends IOApp.Simple:
  implicit val loggerFactory: LoggerFactory[IO] = Slf4jFactory.create[IO]
  def run =
    val service = HttpRoutes.of[IO] {
      case POST -> Root / "sensorA" => Accepted("Hello, World from Http4s A")
      case POST -> Root / "sensorB" => Accepted("Hello, World from Http4s B")
    }

    EmberServerBuilder
      .default[IO]
      .withHost(ipv4"0.0.0.0")
      .withPort(port"8044")
      .withHttpApp(service.orNotFound)
      .build
      .use(_ => IO.never)
