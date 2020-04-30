package org.kidsfirstdrc.dwh.spark.extension

import org.apache.spark.sql.SparkSessionExtensions

class DWHSparkSessionExtension extends Function1[SparkSessionExtensions, Unit] {
  override def apply(v1: SparkSessionExtensions): Unit = v1.injectParser(DWHSparkSessionParser)
}

