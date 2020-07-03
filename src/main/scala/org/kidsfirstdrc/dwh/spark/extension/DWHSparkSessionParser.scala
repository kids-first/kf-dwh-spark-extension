package org.kidsfirstdrc.dwh.spark.extension

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}

case class DWHSparkSessionParser(spark: SparkSession, delegate: ParserInterface) extends ParserInterface {

  var initialized = false
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  private def withExt[T](sqlText: String)(f: String => T) = {
    import spark.implicits._

    if (!initialized) {
      initialized = true
      spark.conf.getOption("spark.kf.dwh.acls").foreach { v =>
        println("Initializing occurences tables...")
        val acls: Map[String, Seq[String]] = mapper.readValue[Map[String, Seq[String]]](v)
        val df = acls.foldLeft(spark.emptyDataFrame) {
          case (currentDF, (study, authorizedConsentCodes)) =>
            val tableName = s"variant.occurences_$study"
            if (spark.catalog.tableExists(tableName)) {
              val nextDF = spark.table(tableName).where($"dbgap_consent_code".isin(authorizedConsentCodes: _*))
              if (currentDF.isEmpty)
                nextDF
              else
                currentDF.union(nextDF)
            } else {
              currentDF
            }

        }
        df.createOrReplaceTempView("occurences")
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
}