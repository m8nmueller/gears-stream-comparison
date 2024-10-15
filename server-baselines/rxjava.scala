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

  router.post("/sensorA").handler: ctx =>
    HttpServerResponse response = ctx.response()
    response.putHeader("content-type", "text/plain")
    response.end("Hello World from Vert.x A!")

  router.post("/sensorB").handler: ctx =>
    HttpServerResponse response = ctx.response()
    response.putHeader("content-type", "text/plain")
    response.end("Hello World from Vert.x B!")

  server.requestHandler(router).listen(8045)

