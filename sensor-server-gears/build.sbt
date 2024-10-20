val vGears = "0.2.0+68-a83969ac-SNAPSHOT"
val vJetty = "12.0.13"

ThisBuild / scalaVersion := "3.4.1"

enablePlugins(JavaAppPackaging)

lazy val sensorwatch = (project in file("."))
  .settings(
    name := "sensorwatch",
    libraryDependencies ++= Seq(
      "ch.epfl.lamp" %% "gears" % vGears,
      "org.eclipse.jetty" % "jetty-server" % vJetty, // https://mvnrepository.com/artifact/org.eclipse.jetty/jetty-server
      "org.eclipse.jetty" % "jetty-util-ajax" % vJetty
    )
  )
