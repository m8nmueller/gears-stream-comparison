//> using scala 3.5.0
//> using dep "com.lihaoyi::cask:0.9.4"
//> using dep "com.lihaoyi::requests:0.9.0"
//> using dep "com.github.oshi:oshi-core:6.6.5"

import upickle.default.*

import java.io.File
import java.lang.ProcessBuilder.Redirect
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.compiletime.uninitialized
import scala.io.Source
import cask.main.Routes
import io.undertow.Undertow
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import ClientRunner.ClientConfig
import scala.util.Random
import java.time.LocalDateTime
import oshi.SystemInfo

// === Configuration and Result Types

case class OsLimitConfiguration(sysCheckInterval: Long, maxCpu: Double, maxVirtualMem: Double) derives ReadWriter

case class GlobalServerConfig(runners: Map[String, LocalServerConfig], osLimits: OsLimitConfiguration)
    derives ReadWriter

case class GlobalClientConfig(k6exe: String, serverBaseUrl: String, resultDir: String, osLimits: OsLimitConfiguration)
    derives ReadWriter

object K6Runner:
  case class Stage(duration: String, number: Int):
    def toStagesString(): String = s"$duration:$number"
    def toStagesSeconds() = duration match
      case s"${amount}m" => amount.toIntOption.map(_ * 60)
      case s"${amount}s" => amount.toIntOption
      case _             => None

  extension (s: Seq[Stage])
    def toStagesString() = s.view.map(_.toStagesString()).mkString(",")
    def toStagesSeconds() =
      val options = s.map(_.toStagesSeconds())
      val values = options.flatten
      if values.size != options.size then None else Some(values.sum)

  sealed trait VuConfig derives ReadWriter:
    def toStages(): Seq[Stage]
  case class ConstantVu(phase: VuPhase, rampDownDuration: String) extends VuConfig:
    def toStages(): Seq[Stage] = phase.toStages() :+ Stage(rampDownDuration, 0)
  case class SequentialVu(phases: Seq[VuPhase], rampDownDuration: String) extends VuConfig:
    def toStages(): Seq[Stage] = phases.flatMap(_.toStages()) :+ Stage(rampDownDuration, 0)

  case class VuPhase(number: Int, duration: String, rampUpDuration: String) derives ReadWriter:
    def toStages(): Seq[Stage] = Seq(Stage(rampUpDuration, number), Stage(duration, number))

  case class Summary(durationMs: Long, reqs: Long, reqsRate: Double, failed: Long, durations: ValueStats)
      derives ReadWriter
  case class ValueStats(min: Double, max: Double, med: Double, avg: Double, `p(90)`: Double, `p(95)`: Double)
      derives ReadWriter
end K6Runner

object ServerRunner:
  enum StartFailure derives ReadWriter:
    case UnknownServer(requested: String)
    case AlreadyRunning(running: String, requested: String)

  enum StopFailure derives ReadWriter:
    case AlreadyStopping
    case TerminationFailed
    case ServerFailed(exitCode: Int)
    case WrongServer(running: Option[String], expected: String)

  case class StopResult(name: String, loggedLines: Int, highCpu: Int, highMemory: Int) derives ReadWriter
end ServerRunner

object ClientRunner:
  case class ClientConfig(runName: String, vuConfig: K6Runner.VuConfig) derives ReadWriter

  enum StopFailure derives ReadWriter:
    case AlreadyStopping
    case NoClientRunning
    case StopTimeout
    case ClientFailed(exitCode: Int)

  case class StopResult(k6summary: K6Runner.Summary, dataDir: String, highCpu: Int, highMemory: Int) derives ReadWriter

trait ServerRunner:
  import ServerRunner.*
  def startServer(name: String): Either[StartFailure, Unit]
  def currentServer(): Option[String]
  def stopServer(name: String): Either[StopFailure, StopResult]

trait ClientRunner:
  import ClientRunner.*
  def startClient(config: ClientConfig): Boolean // false = already running
  def cancel(): Boolean
  def waitClient(): Either[StopFailure, StopResult]

case class LocalServerConfig(name: String, dir: String, exe: String) derives ReadWriter:
  def run(onDone: Boolean => Unit) = RunningApplication(
    ProcessBuilder(exe)
      .directory(File(dir))
      .redirectErrorStream(true)
      .redirectOutput(Redirect.DISCARD)
      .start(),
    onDone
  )
end LocalServerConfig

// === Implementation of low-level tasks

class RunningApplication(proc: Process, onDone: Boolean => Unit):
  private var shouldQuit = false

  proc.onExit().thenAccept(proc_ => if !shouldQuit then onDone(proc_.exitValue() == 0))

  def cancel(): Unit =
    shouldQuit = true
    proc.destroy()

  def quit(): Option[Int] =
    shouldQuit = true
    proc.destroy()
    if proc.waitFor(3, TimeUnit.SECONDS) then Some(proc.exitValue())
    else None

  def waitFor(time: Long, unit: TimeUnit) =
    shouldQuit = true
    var res = proc.waitFor(time, unit)
    if !res then
      shouldQuit = false
      res = !proc.isAlive()

    if res then Some(proc.exitValue()) else None
  end waitFor
end RunningApplication

class OsObservation(config: OsLimitConfiguration):
  private val osBean =
    ManagementFactory.getOperatingSystemMXBean().asInstanceOf[com.sun.management.OperatingSystemMXBean]
  private val memInfo = new SystemInfo().getHardware().getMemory().getVirtualMemory()

  private var observationThread: Thread = uninitialized
  private var cpuHighCounter: Int = uninitialized
  private var memHighCounter: Int = uninitialized

  def start() =
    cpuHighCounter = 0
    memHighCounter = 0
    observationThread = Thread: () =>
      try
        while true do
          if osBean.getCpuLoad() > config.maxCpu then cpuHighCounter += 1
          if memInfo.getVirtualInUse() / osBean.getTotalMemorySize() > config.maxVirtualMem then memHighCounter += 1
          Thread.sleep(config.sysCheckInterval)
      catch case _: InterruptedException => ()
    observationThread.start()
  end start

  /** Stop watching and return the statistics
    *
    * @return
    *   (cpu, mem) high counters
    */
  def stop() =
    observationThread.interrupt()
    observationThread.join()
    (cpuHighCounter, memHighCounter)
end OsObservation

class LocalServerRunner(serverConfig: GlobalServerConfig, onStop: Boolean => Unit) extends ServerRunner:
  import ServerRunner.*
  private val osObservation = OsObservation(serverConfig.osLimits)

  private var stopping = false
  private var runningConfig: LocalServerConfig = null
  private var runningHandle: RunningApplication = uninitialized

  private def logFile = File(runningConfig.dir + "/log.txt")

  private def doStart() =
    // we are safe against concurrent start (runningConfig is not null) and concurrent quit (runningHandle is null)
    logFile.delete()
    osObservation.start()
    runningHandle = runningConfig.run(onStop)

  private def cleanupServer() =
    val logFileLength = Try(Source.fromFile(logFile).getLines().foldLeft(0)((a, _) => a + 1)).getOrElse(0)

    val (cpu, mem) = osObservation.stop()
    val result = StopResult(runningConfig.name, logFileLength, cpu, mem)

    runningHandle = null
    runningConfig = null
    stopping = false

    result
  end cleanupServer

  def startServer(name: String): Either[StartFailure, Unit] = name match
    case serverConfig.runners(config) =>
      val other = this.synchronized:
        if runningConfig == null then
          runningConfig = config
          null
        else runningConfig

      if other == null then
        doStart()
        Right(())
      else Left(StartFailure.AlreadyRunning(running = other.name, requested = name))
    case _ => Left(StartFailure.UnknownServer(name))
  end startServer

  def stopServer(name: String): Either[StopFailure, StopResult] =
    val config = this.synchronized:
      if stopping then return Left(StopFailure.AlreadyStopping)
      if runningConfig == null then Left(StopFailure.WrongServer(running = None, expected = name))
      stopping = true
      runningConfig

    if runningHandle == null then
      stopping = false
      return Left(StopFailure.WrongServer(running = None, expected = name))

    runningHandle.quit() match
      case None =>
        stopping = false
        Left(StopFailure.TerminationFailed)
      case Some(value) =>
        val res = cleanupServer()
        // else Left(StopFailure.ServerFailed(value))
        Right(res)
  end stopServer

  def currentServer(): Option[String] =
    val current = runningConfig
    if current != null then Some(current.name) else None
end LocalServerRunner

class LocalClientRunner(config: GlobalClientConfig, script: String, doneHandler: Boolean => Unit) extends ClientRunner:
  import ClientRunner.*
  import ClientRunner.StopFailure.*

  private val osObservation = OsObservation(config.osLimits)

  private var waiting = false
  private var running: ClientConfig = null
  private var app: RunningApplication = null

  def startClient(config: ClientConfig): Boolean =
    val stages = config.vuConfig.toStages().toStagesString()
    this.synchronized:
      if running != null then return false
      else running = config
    app = RunningApplication(
      ProcessBuilder(
        this.config.k6exe,
        "run",
        "--out",
        "csv=sensorstats.csv",
        "--stage",
        stages,
        script,
        "--env",
        s"SENSOR_SERVER_BASE=${this.config.serverBaseUrl}"
      )
        .redirectErrorStream(true)
        .redirectOutput(Redirect.DISCARD)
        .start(),
      doneHandler
    )
    osObservation.start()
    true
  end startClient

  private def endWaiting() =
    app = null
    running = null
    waiting = false

  private def readResult() =
    val (cpu, mem) = osObservation.stop()
    val rawSummary = ujson.read(File("summary.json"))
    val summary = K6Runner.Summary(
      rawSummary("state")("testRunDurationMs").num.toLong,
      rawSummary("metrics")("http_reqs")("values")("count").num.toLong,
      rawSummary("metrics")("http_reqs")("values")("rate").num,
      rawSummary("metrics")("http_req_failed")("values")("passes").num.toLong,
      ujson.transform(rawSummary("metrics")("http_req_duration")("values"), upickle.default.reader[K6Runner.ValueStats])
    )

    val dir = Paths.get(config.resultDir, running.runName)
    Files.createDirectory(dir)
    Files.move(Paths.get("summary.json"), dir.resolve("summary.json"))
    Files.move(Paths.get("sensorstats.csv"), dir.resolve("sensorstats.csv"))

    endWaiting()
    StopResult(summary, dir.toString(), cpu, mem)
  end readResult

  def waitClient(): Either[StopFailure, StopResult] =
    this.synchronized:
      if waiting then return Left(AlreadyStopping)
      if running == null || app == null then return Left(NoClientRunning)
      waiting = true

    app.waitFor(2 * running.vuConfig.toStages().toStagesSeconds().get + 10, TimeUnit.SECONDS) match
      case None =>
        app.cancel()
        app.waitFor(3, TimeUnit.SECONDS)
        endWaiting()
        Left(StopTimeout)
      case Some(value) =>
        if value == 0 then Right(readResult())
        else
          endWaiting()
          Left(ClientFailed(value))
  end waitClient

  def cancel(): Boolean =
    val a = app
    if a == null then false
    else
      a.cancel()
      true
end LocalClientRunner

// === Communication

class ServerClient(baseUrl: String) extends ServerRunner:
  import ServerRunner.*
  def startServer(name: String): Either[StartFailure, Unit] =
    upickle.default.read[Either[StartFailure, Unit]](
      requests.post.stream(s"$baseUrl/start", params = Map("name" -> name))
    )

  def stopServer(name: String): Either[StopFailure, StopResult] =
    upickle.default.read[Either[StopFailure, StopResult]](
      requests.post.stream(s"$baseUrl/stop", params = Map("name" -> name))
    )

  def currentServer(): Option[String] =
    Some(requests.get(s"$baseUrl/current").text()).filter(_ != "-")
end ServerClient

class ServerRoutes(serverConfig: GlobalServerConfig) extends cask.Routes:
  var server: ServerRunner = uninitialized

  private def onServerExited(clientUrl: String)(success: Boolean) =
    requests.post(s"$clientUrl/server-quit", params = Map("success" -> success.toString()))

  private def verifyClient(clientUrl: String) =
    val response = requests.get(s"$clientUrl/status", check = false)
    response.is2xx && response.text() == "client"

  @cask.post("/connect")
  def connect(clientUrl: String): Boolean =
    if server != null then return false
    if !verifyClient(clientUrl) then throw Exception("Client unreachable")
    synchronized:
      if server != null then false
      else
        server = LocalServerRunner(serverConfig, onServerExited(clientUrl))
        true

  @cask.post("/disconnect")
  def disconnect(): Boolean =
    val s = server
    if s == null then return false

    s.currentServer() match
      case Some(value) => throw Exception(s"Cannot disconnect while running $value")
      case None =>
        synchronized:
          if server eq s then
            server = null
            true
          else false
  end disconnect

  @cask.post("/start")
  def start(name: String) = upickle.default.writeJs(server.startServer(name))

  @cask.post("/stop")
  def stop(name: String) = upickle.default.writeJs(server.stopServer(name))

  @cask.get("/current")
  def current() = server.currentServer().getOrElse("-")

  @cask.get("/status")
  def status() = "server"

  initialize()
end ServerRoutes

class ClientRoutes(client: ClientRunner, quitHandler: Boolean => Unit) extends cask.Routes:
  @cask.post("/start")
  def start(request: cask.Request) =
    val body: ClientRunner.ClientConfig = upickle.default.read(request.data, false)
    client.startClient(body)

  @cask.post("/wait")
  def stop() = upickle.default.writeJs(client.waitClient())

  @cask.get("/status")
  def status() = "client"

  @cask.post("/server-quit")
  def onServerQuit(success: Boolean) = quitHandler(success)

  initialize()
end ClientRoutes

// === Controller

case class ControllerConfig(
    tasks: Seq[ControllerTask],
    // checkTask: ClientRunner.ClientConfig,
    cpuWarn: Double, // when cpuWarn / cpuRead exceeds this rate, consider as dangerous
    memWarn: Double, // similarly
    sysCheckInterval: Long,
    warnTries: Int,
    tries: Int
) derives ReadWriter

sealed trait ControllerTask derives ReadWriter:
  def name: String
  def toConfigs(): Seq[ClientRunner.ClientConfig]

case class SimpleTask(name: String, impls: Seq[String], types: Seq[String], configs: Seq[K6Runner.VuConfig])
    extends ControllerTask:
  def toConfigs(): Seq[ClientConfig] =
    for
      impl <- impls
      typ <- types
      (conf, cidx) <- configs.zipWithIndex
    yield ClientConfig(s"$impl-$typ.$cidx.$name", conf)

class Controller(config: ControllerConfig):
  private var client: ClientRunner = uninitialized // synchronize on start and cancel
  private var server: ServerRunner = uninitialized

  private var currentTask: ClientConfig = uninitialized
  private var needRetry: Boolean = uninitialized
  private var warnRetry: Boolean = uninitialized

  def onClientDone(success: Boolean): Unit = ()

  def onServerQuit(success: Boolean): Unit =
    val res = synchronized:
      if currentTask != null then
        needRetry = true
        (currentTask, client.cancel())
      else null

    if res != null then
      val (task, succeeded) = res
      println(
        s"[${task.runName}] Server terminated unexpectedly (success=$success), cancelled client (success=$succeeded)"
      )
    else println(s"Server terminated unexpectedly (success=$success) for unknown task")
  end onServerQuit

  private def checkResults(
      clientRes: Either[ClientRunner.StopFailure, ClientRunner.StopResult],
      serverRes: Either[ServerRunner.StopFailure, ServerRunner.StopResult]
  ) =
    if clientRes.isLeft then println(s"[${currentTask.runName}] Client failed: ${clientRes.left.get}")
    if serverRes.isLeft then println(s"[${currentTask.runName}] Server failed: ${serverRes.left.get}")

    (clientRes, serverRes) match
      case (Right(clientData), Right(serverData)) =>
        val checks = clientData.k6summary.durationMs / config.sysCheckInterval
        val clientExceeded =
          (clientData.highCpu / checks > config.cpuWarn) || (clientData.highMemory / checks > config.memWarn)
        val serverExceeded =
          (serverData.highCpu / checks > config.cpuWarn) || (serverData.highMemory / checks > config.memWarn)

        val dir =
          if clientExceeded || serverExceeded then
            val newDir = clientData.dataDir + "-" + Random.alphanumeric.filter(_.isLetter).take(6).mkString
            Files.move(Paths.get(clientData.dataDir), Paths.get(newDir))
            newDir
          else clientData.dataDir

        val sum = clientData.k6summary
        println(
          s"[${currentTask.runName}] ${sum.reqs}reqs/${sum.durationMs}ms = ${sum.reqsRate}RPS (${sum.failed} failed, mean duration=${sum.durations.avg}ms) @$dir"
        )

        if clientExceeded || serverExceeded then
          val cs = if clientExceeded then "Client" else ""
          val ss = if serverExceeded then (if clientExceeded then " and server" else "Server") else ""

          println(s"[${currentTask.runName}] $cs$ss exceeded system limits")
          needRetry = true
          warnRetry = true
      case (Right(clientData), Left(_)) =>
        val newDir = clientData.dataDir + "-" + Random.alphanumeric.filter(_.isLetter).take(6).mkString
        Files.move(Paths.get(clientData.dataDir), Paths.get(newDir))
        needRetry = true
      case _ => needRetry = true
  end checkResults

  private def runConfig(config: ClientConfig) =
    synchronized:
      if currentTask != null then throw IllegalStateException("currentTask not null")
      currentTask = config
    needRetry = false
    warnRetry = false

    val serverName = config.runName.split("\\.", 2)(0)
    server.startServer(serverName) match
      case Left(failure) =>
        println(s"[${config.runName}] Failed to start: $failure")
        failure match
          case ServerRunner.StartFailure.AlreadyRunning(running, requested) =>
            server.stopServer(running) match
              case Left(value) =>
                println(s"[${config.runName}] Unable to stop $running to run this: $value")
              case Right(value) =>
                println(s"[${config.runName}] Successfully stopped ${value.name} to run this")
            needRetry = true
          case _ => ()
      case Right(_) =>
        if !synchronized(client.startClient(config)) then
          val stopResult = server.stopServer(serverName)
          synchronized(client.cancel())
          println(s"[${config.runName}] Unable to start client, stopped server: ${stopResult}")

        val clientRes = client.waitClient()
        val serverRes = server.stopServer(serverName)
        checkResults(clientRes, serverRes)

    synchronized { currentTask = null }
  end runConfig

  private def start0() =
    val tasks = config.tasks.flatMap(_.toConfigs())
    println(s"Found ${tasks.size} tasks")

    val totalSeconds = tasks.map(_.vuConfig.toStages().toStagesSeconds().get).sum
    val eta = LocalDateTime.now().plusSeconds(totalSeconds)
    println(s"Estimated ETA: $eta")

    for ctask <- config.tasks do
      println(s"[Task ${ctask.name}]")
      for cc <- ctask.toConfigs() do
        needRetry = true
        var tries = config.tries
        var warnTries = config.warnTries

        while needRetry && tries > 0 && warnTries > 0 do
          runConfig(cc)
          tries -= 1
          if warnRetry then warnTries -= 1

        if (needRetry) println(s"${cc.runName} failed after retrying")
  end start0

  def start(client: ClientRunner, server: ServerRunner) =
    synchronized:
      if this.client != null then throw IllegalStateException()
      this.client = client
      this.server = server
    start0()
end Controller

// === main methods

@main
def server(configFile: String) =
  val config: GlobalServerConfig = upickle.default.read(File(configFile), true)
  MainApp("server-computer.temp", 8081, Seq(ServerRoutes(config))).run()

case class ClientManagerConfig(selfUrl: String, serverUrl: String, runner: GlobalClientConfig, script: String)
    derives ReadWriter

@main
def client(configFile: String, taskFile: String) =
  val config: ClientManagerConfig = upickle.default.read(File(configFile), true)
  val controllerConfig: ControllerConfig = upickle.default.read(File(taskFile), true)

  val controller = Controller(controllerConfig)
  val runner = LocalClientRunner(config.runner, config.script, controller.onClientDone)

  val routes = ClientRoutes(runner, controller.onServerQuit)
  val serverThread = new Thread(() => MainApp("client-computer.temp", 8082, Seq(routes)).run())
  serverThread.start()

  var counter = 0
  while counter >= 0 do
    Try(requests.get(s"${config.selfUrl}/status", connectTimeout = 200)) match
      case Failure(exception) =>
        println("Failed to request self")
        exception.printStackTrace()
        counter += 1
        if counter == 5 then
          println("Startup timed out, quitting...")
          System.exit(1)
        Thread.sleep(500)
      case Success(value) =>
        println("Client HTTP started, connecting to server...")
        val res = requests.post(s"${config.serverUrl}/connect", params = Map("clientUrl" -> config.selfUrl))
        if res.statusCode != 200 || res.text() != "true" then
          println("Connecting failed, quitting...")
          System.exit(2)
        counter = -1
  end while

  println("Starting controller...")
  try controller.start(runner, ServerClient(config.serverUrl))
  finally
    serverThread.interrupt()
    Try(requests.post(s"${config.serverUrl}/disconnect").text() match
      case "true" => ()
      case other  => println(s"Failed to disconnect: $other")
    ).failed.foreach: t =>
      println("Disconnect failed:")
      t.printStackTrace()
  System.exit(0)
end client

class MainApp(appHost: String, appPort: Int, routes: Seq[Routes]) extends cask.Main:
  def allRoutes: Seq[Routes] = routes
  override def host: String = appHost
  override def port: Int = appPort
  def run() = super.main(Array())
