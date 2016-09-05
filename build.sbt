name := "RealTimeRecommender"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "com.eed3si9n" % "sbt-assembly_2.8.1" % "sbt0.10.1_0.6" % "provided"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.1" % "provided"
libraryDependencies += "org.apache.commons" % "commons-email" % "1.4"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"
libraryDependencies += "com.microsoft.azure" % "azure-storage" % "4.0.0"

    