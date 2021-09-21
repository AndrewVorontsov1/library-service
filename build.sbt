name := "library-service"

version := "0.1"

scalaVersion := "2.13.6"

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value
)
libraryDependencies += "com.github.fd4s" %% "fs2-kafka" % "2.2.0"


