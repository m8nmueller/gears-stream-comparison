//> using scala 3.5.0
//> using dep io.vertx:vertx-web:4.5.10

import io.vertx.core.Vertx
import io.vertx.ext.web.Router

import scala.jdk.CollectionConverters.*

@main
def hello() =
  val vertx = Vertx.vertx()
  val server = vertx.createHttpServer()
  val router = Router.router(vertx)

  router
    .post("/sensorA")
    .handler: ctx =>
      val response = ctx.response()
      response.setStatusCode(202)
      response.end()

  router
    .post("/sensorB")
    .handler: ctx =>
      val response = ctx.response()
      response.setStatusCode(202)
      response.end()

  server.requestHandler(router).listen(8044)
