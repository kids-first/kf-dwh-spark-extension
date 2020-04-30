package org.kidsfirstdrc.dwh.spark.extension.testutils

import java.io.File
import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

trait WithSparkSession {

  private val tmp = new File("tmp").getAbsolutePath

  def getSparkSessionBuilder(): SparkSession.Builder = {
    SparkSession.builder()
      .config("spark.ui.enabled", value = false)
      .config("spark.sql.warehouse.dir", s"$tmp/wharehouse")
      .config("spark.driver.extraJavaOptions", s"-Dderby.system.home=$tmp/derby")
      .enableHiveSupport()
      .master("local")

    }


  def withOutputFolder[T](prefix: String)(block: String => T): T = {
    val output: Path = Files.createTempDirectory(prefix)
    try {
      block(output.toAbsolutePath.toString)
    } finally {
      FileUtils.deleteDirectory(output.toFile)
    }
  }
}
