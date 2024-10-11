package sensorwatch.architecture

import gears.async.Async
import gears.async.ChannelClosedException
import gears.async.Future
import gears.async.Semaphore
import gears.async.stream.PushDestination
import gears.async.stream.PushLayers
import gears.async.stream.PushSenderStream
import gears.async.stream.StreamResult
import gears.async.stream.StreamResult.StreamTerminatedException
import gears.async.stream.StreamSender

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousCloseException
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.ClosedChannelException
import java.nio.channels.CompletionHandler
import java.nio.charset.Charset
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicLong
import scala.util.Failure

class LockCell[V](var value: V, val lock: Semaphore)

extension [T](s: PushSenderStream[T])
  def mapAsync[V](fn: T => Async ?=> V): s.ThisStream[V] =
    new PushLayers.SenderMixer[T, V](s) with PushLayers.SingleDestTransformer[StreamSender, T, V]:
      def transformSingle(sender: StreamSender[V]): StreamSender[T] =
        new PushLayers.ForwardTerminate[T] with PushLayers.ToSender(sender):
          def send(x: T)(using Async): Unit =
            downstream.send(fn(x))
  end mapAsync

  def tap(fn: T => Async ?=> Unit): s.ThisStream[T] =
    new PushLayers.SenderMixer[T, T](s) with PushLayers.SingleDestTransformer[StreamSender, T, T]:
      def transformSingle(sender: StreamSender[T]): StreamSender[T] =
        new PushLayers.ForwardTerminate[T] with PushLayers.ToSender(sender):
          def send(x: T)(using Async): Unit =
            fn(x)
            downstream.send(x)
  end tap

  def scan[V](v0: V)(fn: (V, T) => V): s.ThisStream[V] =
    new PushSenderStream[V]:
      def transformSingle(cell: LockCell[V], sender: StreamSender[V]): StreamSender[T] & PushLayers.ToSender[V] =
        new PushLayers.ForwardTerminate[T] with PushLayers.ToSender(sender):
          def send(x: T)(using Async): Unit =
            val guard = cell.lock.acquire()
            var value: V = cell.value

            try
              value = fn(value, x)
              cell.value = value
            finally guard.release()
            downstream.send(value)
      end transformSingle

      def runToSender(sender: PushDestination[StreamSender, V])(using Async): Unit =
        val cell = LockCell(v0, Semaphore(1))

        var first: StreamSender[V] = null
        val upsenders = PushLayers.handleMaybeIt(sender) { single =>
          first = single
          transformSingle(cell, single)
        } { iterator =>
          first = iterator.next()
          (Iterator(first) ++ iterator).map(transformSingle(cell, _))
        }

        try first.send(v0)
        catch
          case e: StreamTerminatedException =>
            e.getCause() match
              case null  => return
              case cause => throw cause
          case _: ChannelClosedException => ()

        s.runToSender(upsenders)
      end runToSender
  end scan

  def movingWindow(size: Int): s.ThisStream[List[T]] =
    scan(List.empty[T]): (xs, x) =>
      if xs.length < size then (x +: xs)
      else x +: xs.init
    .filter(_.size == size)

  def chunkN(n: Int): s.ThisStream[Seq[T]] =
    new PushLayers.SenderMixer[T, Seq[T]](s) with PushLayers.DestTransformer[StreamSender, T, Seq[T]]:
      def transform(sender: PushDestination[StreamSender, Seq[T]]): PushDestination[StreamSender, T] =
        val chunker = LockCell(Seq.newBuilder[T], Semaphore(1))
        chunker.value.sizeHint(n)

        PushLayers.mapMaybeIt(sender): single =>
          new PushLayers.ForwardTerminate[T] with PushLayers.ToSender(single):
            def send(x: T)(using Async): Unit =
              val guard = chunker.lock.acquire()
              var builder = chunker.value
              try
                builder += x
                if builder.knownSize < n then builder = null
                else
                  chunker.value = Seq.newBuilder
                  chunker.value.sizeHint(n)
              finally guard.release()

              if builder != null then downstream.send(builder.result())
  end chunkN
end extension

extension (s: PushSenderStream[ByteBuffer])
  def writeToFile(file: Path, options: OpenOption*): s.ThisStream[Unit] = new PushSenderStream:
    def runToSender(sender: PushDestination[StreamSender, Unit])(using Async): Unit =
      val fc = AsynchronousFileChannel.open(file, (StandardOpenOption.WRITE +: options)*)

      val upsender = new StreamSender[ByteBuffer]:
        val pos = AtomicLong(fc.size())
        def send(x: ByteBuffer)(using Async): Unit =
          val writePos = pos.getAndAdd(x.remaining())
          Future
            .withResolver(fc.write(x, writePos, _, completionHandler))
            .awaitResult
            .recoverWith { case _: AsynchronousCloseException | _: ClosedChannelException =>
              Failure(ChannelClosedException())
            }
            .get
        end send

        def terminate(value: StreamResult.Done): Boolean =
          val terminated = synchronized:
            if fc.isOpen() then
              fc.close()
              true
            else false

          if terminated then PushLayers.handleMaybeIt(sender)(_.terminate(value))(_.next().terminate(value))
          terminated
        end terminate
      end upsender

      try s.runToSender(upsender)
      finally fc.close()
    end runToSender
  end writeToFile
end extension

private final def completionHandler[T]: CompletionHandler[T, Future.Resolver[T]] = new CompletionHandler:
  def completed(result: T, attachment: Future.Resolver[T]): Unit = attachment.resolve(result)
  def failed(exc: Throwable, attachment: Future.Resolver[T]): Unit = attachment.reject(exc)
