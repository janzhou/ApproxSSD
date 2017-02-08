name := "nvmr"
scalaVersion := "2.11.7"
organization := "org.janzhou"

enablePlugins(GitVersioning)

scalacOptions ++= Seq("-optimise", "-feature", "-deprecation")

libraryDependencies += "net.java.dev.jna" % "jna" % "4.2.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.1"
libraryDependencies += "com.esotericsoftware" % "kryo" % "3.0.3"

resolvers += "janzhou-github-mvn-repo" at "https://raw.githubusercontent.com/janzhou/mvn-repo/master"
resolvers += "janzhou-bitbucket-mvn-repo" at "https://bitbucket.org/janzhou/mvn-repo/raw/master"
libraryDependencies += "org.janzhou" %% "native" % "0.1.2"

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/mvn-repo")))
