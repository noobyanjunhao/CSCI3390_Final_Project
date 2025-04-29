name := "CSCI3390_Final_Project"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-graphx" % "3.3.2"
)
