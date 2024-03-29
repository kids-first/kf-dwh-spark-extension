package org.kidsfirstdrc.dwh.spark.extension

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.functions.not
import org.apache.spark.sql.types.{DataType, StructType}

case class DWHSparkSessionParser(spark: SparkSession, delegate: ParserInterface) extends ParserInterface {

  var initialized = false
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  val PUBLIC_CONSENT_CODE = "_PUBLIC_"

  private def createViewsFor(tablePrefix: String, acls_str: String)(spark: SparkSession): Unit = {
    import spark.implicits._
    println(s"Initializing $tablePrefix tables...")
    val acls: Map[String, Seq[String]] = mapper.readValue[Map[String, Seq[String]]](acls_str)
    val allOccurrenceTables: Array[String] = spark.sql("show tables in variant").select("tableName")
      .where($"tableName" like s"${tablePrefix}_sd%" and not($"tableName" like "%_re_0%"))
      .as[String].collect()
      .map(t => s"variant.$t")

    val (authorizedTables, unauthaurizedTables) = acls.foldLeft((spark.emptyDataFrame, allOccurrenceTables)) {
      case ((currentDF, remainingTables), (study, authorizedConsentCodes)) =>
        val tableName = s"variant.${tablePrefix}_${study.toLowerCase}"
        if (remainingTables.contains(tableName)) {
          val nextRemainingTables = remainingTables.filterNot(_ == tableName)
          val nextDF = if (authorizedConsentCodes.nonEmpty) {
            spark.table(tableName).where($"dbgap_consent_code".isin(authorizedConsentCodes :+ PUBLIC_CONSENT_CODE: _*))
          } else {
            spark.table(tableName)
          }
          if (currentDF.isEmpty)
            (nextDF, nextRemainingTables)
          else
            (currentDF.unionByName(nextDF), nextRemainingTables)
        } else {
          (currentDF, remainingTables)
        }

    }
    val withPublics = unauthaurizedTables.foldLeft(authorizedTables) {
      case (currentDF, tableName) =>
        val nextDF = spark.table(tableName).where($"dbgap_consent_code" === PUBLIC_CONSENT_CODE)
        if (currentDF.isEmpty)
          nextDF
        else
          currentDF.unionByName(nextDF)
    }
    withPublics.createOrReplaceTempView(tablePrefix)
  }

  private def withExt[T](sqlText: String)(f: String => T) = {
    if (!initialized) {
      initialized = true
      spark.conf.getOption("spark.kf.dwh.acls").foreach { acls =>
        createViewsFor("occurrences", acls)(spark)
        createViewsFor("occurrences_family", acls)(spark)
      }
      spark.sql("use variant_live")
    }

    replace(sqlText, f)

  }

  private def replace[T](sqlText: String, f: String => T) = {
    val savedSets = spark.conf.getOption("spark.kf.dwh.saved_sets")
    val replaced = savedSets.map(t => sqlText
      .replaceAll("\\bsaved_sets\\b", s"json.`$t`")).getOrElse(sqlText)
    f(replaced)
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier = withExt(sqlText)(delegate.parseTableIdentifier)

  override def parsePlan(sqlText: String): LogicalPlan = withExt(sqlText)(delegate.parsePlan)

  override def parseExpression(sqlText: String): Expression = withExt(sqlText)(delegate.parseExpression)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = withExt(sqlText)(delegate.parseFunctionIdentifier)

  override def parseTableSchema(sqlText: String): StructType = withExt(sqlText)(delegate.parseTableSchema)

  override def parseDataType(sqlText: String): DataType = withExt(sqlText)(delegate.parseDataType)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = withExt(sqlText)(delegate.parseMultipartIdentifier)

  override def parseRawDataType(sqlText: String): DataType = withExt(sqlText)(delegate.parseRawDataType)
}
