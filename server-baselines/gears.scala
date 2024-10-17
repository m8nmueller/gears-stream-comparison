//> using scala 3.5.0
//> using dep org.eclipse.jetty:jetty-server:12.0.13

package sensorwatch.architecture

import org.eclipse.jetty.http.pathmap.PathSpec
import org.eclipse.jetty.server.Handler
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.Response
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.handler.PathMappingsHandler
import org.eclipse.jetty.util.Callback
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.eclipse.jetty.util.thread.VirtualThreadPool

import java.nio.ByteBuffer

extension (server: Server)
  private def run() =
    server.start()
    server.join()
end extension

private def mkServer(handler: Handler, port: Int) =
  val threadPool = new QueuedThreadPool()

  // Use the bounded VirtualThreadPool with a custom executor to start Futures
  val virtualExecutor = new VirtualThreadPool()
  virtualExecutor.setMaxThreads(128)

  // Sets threadPool's reservedThreads = 0 to always use virtual threads
  threadPool.setVirtualThreadsExecutor(virtualExecutor)

  val server = new Server(threadPool)
  server.setHandler(handler)

  val connector = new ServerConnector(server)
  connector.setPort(port)
  server.addConnector(connector)

  server
end mkServer

@main
def run() =
  val handlers = PathMappingsHandler(true)
  handlers.addMapping(
    PathSpec.from("/sensorA"),
    new Handler.Abstract:
      def handle(request: Request, response: Response, callback: Callback): Boolean =
        response.setStatus(202)
        response.write(true, ByteBuffer.wrap("Hello World from Jetty A".getBytes()), callback)
        true
  )
  handlers.addMapping(
    PathSpec.from("/sensorB"),
    new Handler.Abstract:
      def handle(request: Request, response: Response, callback: Callback): Boolean =
        response.setStatus(202)
        response.write(true, ByteBuffer.wrap("Hello World from Jetty B".getBytes()), callback)
        true
  )
  mkServer(handlers, 8044).run()
