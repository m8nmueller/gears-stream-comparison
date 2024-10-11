package sensorwatch.architecture

import gears.async.Async
import gears.async.ChannelClosedException
import gears.async.Future
import gears.async.stream.PushDestination
import gears.async.stream.PushSenderStream
import gears.async.stream.StreamOps
import gears.async.stream.StreamResult
import gears.async.stream.StreamSender
import org.eclipse.jetty.http.Trailers
import org.eclipse.jetty.http.pathmap.PathSpec
import org.eclipse.jetty.io.Content
import org.eclipse.jetty.server.Handler
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.Response
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.handler.PathMappingsHandler
import org.eclipse.jetty.util.Callback
import org.eclipse.jetty.util.ajax.AsyncJSON
import org.eclipse.jetty.util.component.LifeCycle
import org.eclipse.jetty.util.thread.Invocable.InvocationType
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.eclipse.jetty.util.thread.VirtualThreadPool

import java.nio.ByteBuffer
import java.util.concurrent.Executor
import java.util.concurrent.LinkedBlockingQueue
import scala.util.Success
import scala.util.boundary

extension (s: Future.type)
  def withCallback(register: Callback => Unit): Future[Unit] =
    Future.withResolver: resolv =>
      register(new Callback {
        override def getInvocationType(): InvocationType = InvocationType.NON_BLOCKING
        override def succeeded(): Unit = resolv.resolve(())
        override def failed(x: Throwable): Unit = resolv.reject(x)
      })

class HttpResponseHandle(val response: Response, val callback: Callback):
  def write(last: Boolean, byteBuffer: ByteBuffer)(using Async) =
    Future.withCallback(response.write(last, byteBuffer, _)).await
  def finish(): Unit = callback.succeeded()

class HttpHandle(val request: Request, response: Response, callback: Callback)
    extends HttpResponseHandle(response, callback)

class HttpStreamException(cause: Throwable) extends Exception(cause)

object FutureExecutor:
  private[architecture] val asyncInstance = ScopedValue.newInstance[Async]()

private class FutureExecutor(spawn: Async.Spawn) extends Executor:
  def execute(command: Runnable): Unit = Future { fAsync ?=>
    ScopedValue.where(FutureExecutor.asyncInstance, fAsync).run(command)
  }(using spawn, spawn)

extension (server: Server)
  private def run()(using Async) =
    server.start()
    Future
      .withResolver[Unit]: resolv =>
        server.addEventListener(new LifeCycle.Listener:
          override def lifeCycleFailure(event: LifeCycle, cause: Throwable): Unit =
            resolv.reject(cause)
          override def lifeCycleStopped(event: LifeCycle): Unit =
            resolv.resolve(())
        )
      .await
end extension

private def mkServer(exec: Executor, handler: Handler, port: Int) =
  val threadPool = new QueuedThreadPool()

  // Use the bounded VirtualThreadPool with a custom executor to start Futures
  val virtualExecutor = new VirtualThreadPool()
  virtualExecutor.setMaxThreads(32)
  virtualExecutor.setVirtualThreadsExecutor(exec)

  // Sets threadPool's reservedThreads = 0 to always use virtual threads
  threadPool.setVirtualThreadsExecutor(virtualExecutor)

  val server = new Server(threadPool)
  server.setHandler(handler)

  val connector = new ServerConnector(server)
  connector.setPort(port)
  server.addConnector(connector)

  server
end mkServer

private trait BlockingSenderPool[T]:
  def withSender(use: StreamSender[T] => Unit): Unit

private class QueuedSenderPool[T](it: Iterator[StreamSender[T]]) extends BlockingSenderPool[T]:
  private val queue = LinkedBlockingQueue[StreamSender[T]]()
  def withSender(use: StreamSender[T] => Unit): Unit =
    var sender = queue.poll()
    if sender == null then
      synchronized:
        if it.hasNext then sender = it.next()
      if sender == null then sender = queue.take()
    try use(sender)
    finally queue.add(sender)

private class SingleSenderPool[T](sender: StreamSender[T]) extends BlockingSenderPool[T]:
  def withSender(use: StreamSender[T] => Unit): Unit = use(sender)

class HttpStreams(port: Int):
  private var streams: Seq[PushSenderStream[?]] = null
  private val handlers = PathMappingsHandler(true /*dynamic*/ )
  private var serverRunning: Future.Promise[Unit] = null

  private var failure: Throwable = null // synchronized via object monitor

  private class NoOpSender extends StreamSender[Any]:
    var closed = false // synchronized via this sender's object monitor
    def send(_x: Any)(using Async): Unit =
      if closed then throw ChannelClosedException()
    def terminate(value: StreamResult.Done): Boolean =
      val effective = synchronized:
        if closed then false
        else
          closed = true
          true
      if effective && value.isInstanceOf[Throwable] then
        HttpStreams.this.synchronized:
          if failure == null then failure.asInstanceOf[Throwable]
      effective
  end NoOpSender

  private def waitForServer() = synchronized:
    if serverRunning != null then serverRunning
    else
      serverRunning = Future.Promise()
      null

  private def shutdownHandler(server: Server) = new Handler.Abstract.NonBlocking:
    def handle(request: Request, response: Response, callback: Callback): Boolean =
      response.write(true, ByteBuffer.allocate(0), callback)
      server.stop()
      true

  def onPath(pathSpec: String): PushSenderStream[HttpHandle] = new PushSenderStream:
    def runToSender(sender: PushDestination[StreamSender, HttpHandle])(using Async): Unit =
      if streams == null then throw IllegalStateException("stream must be started via HttpStreams instance")

      val pool = sender match
        case single: StreamSender[?] => SingleSenderPool(single.asInstanceOf[StreamSender[HttpHandle]])
        case iterator: Iterator[?]   => QueuedSenderPool(iterator.asInstanceOf[Iterator[StreamSender[HttpHandle]]])

      val handler = new Handler.Abstract:
        def handle(request: Request, response: Response, callback: Callback): Boolean =
          pool.withSender: sender =>
            sender.send(HttpHandle(request, response, callback))(using FutureExecutor.asyncInstance.get())
          true

      handlers.synchronized(handlers.addMapping(PathSpec.from(pathSpec), handler))

      val element = HttpStreams.this.synchronized:
        if streams.isEmpty then null
        else
          val head = streams.head
          streams = streams.tail
          head

      if element != null then element.runToSender(new NoOpSender)
      else
        val waitable = waitForServer()
        if waitable != null then waitable.await
        else
          Async.group: spawn ?=>
            val server = mkServer(FutureExecutor(spawn), handlers, port)
            handlers.synchronized(handlers.addMapping(PathSpec.from("/shutdown"), shutdownHandler(server)))
            server.setStopAtShutdown(true)
            server.run()
            serverRunning.complete(Success(()))
  end onPath

  def drainAll(streams: PushSenderStream[?]*)(using Async): Unit =
    require(streams.nonEmpty, "streams empty")
    this.streams = streams.tail
    streams.head.runToSender(new NoOpSender)
    if failure != null then throw HttpStreamException(failure)
end HttpStreams

extension (s: StreamOps[HttpHandle])
  def filterHttp(failureCode: Int, failureMessage: String)(test: HttpHandle => Boolean): s.ThisStream[HttpHandle] =
    s.filter: handle =>
      if test(handle) then true
      else
        Response.writeError(handle.request, handle.response, handle.callback, 404, "This is almost good!")
        false

private val defaultFactory = AsyncJSON.Factory()

extension (request: Request)
  def readBody(handleChunk: Content.Chunk => Async ?=> Unit, handleTrailers: Trailers => Async ?=> Unit)(using Async) =
    boundary:
      while true do
        request.read() match
          case null => Future.withResolver(resolv => request.demand(() => resolv.resolve(()))).await
          case chunk if Content.Chunk.isFailure(chunk) => throw chunk.getFailure()
          case trailers: Trailers =>
            handleTrailers(trailers)
            boundary.break(())
          case chunk =>
            handleChunk(chunk)
            chunk.release()
            if chunk.isLast() then boundary.break(())

  def asJson()(using Async): java.util.Map[String, Object] =
    val parser = defaultFactory.newAsyncJSON()
    readBody(chunk => parser.parse(chunk.getByteBuffer()), _ => {})
    parser.complete()
