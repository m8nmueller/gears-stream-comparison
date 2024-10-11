//> using scala 3.5.0
//> using dep "com.softwaremill.sttp.client3::core:3.10.0"

import sttp.client3.*
import sttp.model.MediaType
import sttp.model.Uri

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.FileReader
import java.io.FileWriter
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*
import scala.util.Success
import scala.util.Failure
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random
import scala.util.Using
import java.util.concurrent.Semaphore

val dataPath = "./sensordata.txt"

@main
def generator(
    count: Int,
    probA: Double,
    sizeA: Int,
    probAsucc: Double,
    avgA: Double,
    stdA: Double,
    devMinA: Double,
    devMaxA: Double,
    minB: Int,
    maxB: Int,
    avgB: Double,
    stdB: Double
) =
  val diffMs = 250

  val random = Random()

  Using(new FileWriter(dataPath)): writer =>
    for pos <- 0 until count do
      val time = pos * diffMs

      if random.nextDouble() < probA then
        // generate an A
        val arr = Array.ofDim[(Double, Double)](sizeA)
        for idx <- arr.indices do
          if random.nextDouble() < probAsucc then
            val reading = random.nextGaussian() * stdA + avgA
            val deviation = random.between(devMinA, devMaxA)
            arr(idx) = (reading, deviation)

        val measurementsString = arr.view
          .map {
            case null                 => "null"
            case (reading, deviation) => s"{\"reading\":$reading,\"deviation\":$deviation}"
          }
          .mkString(",")
        writer.write(s"A{\"timestamp\":$time,\"measurements\":[$measurementsString]}\n")
      else
        // generate a B
        val count = random.between(minB, maxB)
        val arr = Array.fill(count)(random.nextGaussian() * stdB + avgB)
        writer.write(s"B{\"timestamp\":$time,\"measurements\":[${arr.mkString(",")}]}\n")
  println(s"Wrote $count sensor payloads to $dataPath")

def mkCooldown(desc: String): Int => Unit = desc match
  case s"constant:$every:$millis" => (now) => if now % every.toInt == 0 then Thread.sleep(millis.toLong)
  case "none"                     => (now) => ()
  case _                          => throw IllegalArgumentException("unknown cooldown specifier")

@main
def runner(baseUrl: String, cooldownDescription: String, parallelReqNum: Int, parallelGain: Int) =
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val urls = Map(
    'A' -> "/sensorA/",
    'B' -> "/sensorB/"
  ).map((sensor, path) => (sensor, Uri.parse(baseUrl + path).right.get))
  val cooldown = mkCooldown(cooldownDescription)

  val reader = new BufferedReader(new FileReader(dataPath))
  val backend = HttpClientFutureBackend()

  val stats = ConcurrentLinkedQueue[(Long, Long)]()
  val parallelRequests = new Semaphore(parallelReqNum)
  val succCounter = AtomicInteger(0)

  @tailrec
  def run(num: Int): Int =
    val line = reader.readLine()
    if line != null && !line.isEmpty() then
      val req = emptyRequest
        .body(line.substring(1))
        .post(urls(line.charAt(0)))

      parallelRequests.acquire()
      val startTime = System.currentTimeMillis()
      req
        .send(backend)
        .andThen:
          case Success(res) =>
            val duration = System.currentTimeMillis() - startTime
            parallelRequests.release()
            if succCounter.incrementAndGet() % parallelGain == 0 then
              succCounter.getAndAdd(-parallelGain)
              parallelRequests.release()

            if res.code.code != 202 then
              println(s"Warning: response HTTP ${res.code.code}")
              stats.add((startTime, -duration - 1))
            else stats.add((startTime, duration))
          case Failure(ex) =>
            val duration = System.currentTimeMillis() - startTime
            println(s"Warning: request failed: $ex")
            stats.add((startTime, -duration - 1))
      if num % 20 == 0 then print(".")
      cooldown(num)
      run(num + 1)
    else num

  val overallStart = System.currentTimeMillis()
  val num = run(0)
  val overallTime = System.currentTimeMillis() - overallStart

  println("\nDone sending")
  while num > stats.size() do Thread.`yield`()
  println(s"Sent $num requests in $overallTime ms (${stats.iterator().asScala.count((_, time) => time < 0)} failed)")

  val statPath = Files.createTempFile(Paths.get("."), "sensorstats-", ".csv")
  val statString = stats.asScala.toSeq.sortBy(_._1).map((start, duration) => s"$start,$duration").mkString("\n")
  Files.writeString(statPath, statString)
  println(s"Wrote stats to $statPath")
