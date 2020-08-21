package org.kidsfirstdrc.dwh.spark.extension


import org.kidsfirstdrc.dwh.spark.extension.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Path

case class Occurrence(id: Int, dbgap_consent_code: String)

case class SavedSet(id: String, name: String)

class DWHSparkSessionExtensionSpec extends AnyFlatSpec with WithSparkSession with Matchers {


  "use session extension" should "create occurrences view" in {
    val writeSession = getSparkSessionBuilder().getOrCreate()
    import writeSession.implicits._
    writeSession.sql("create database if not exists variant")
    writeSession.sql("create database if not exists variant_live")
    withOutputFolder("work") { work =>

      writeSession.sql("drop table if exists variant.occurrences_sd_1")
      Seq(Occurrence(1, "A"), Occurrence(2, "B"), Occurrence(3, "C")).toDF().write
        .option("path", s"$work/occurrences_sd_1")
        .format("json")
        .saveAsTable("variant.occurrences_sd_1")

      writeSession.sql("drop table if exists variant.occurrences_sd_2")
      Seq(Occurrence(4, "A"), Occurrence(5, "D"), Occurrence(9, "_PUBLIC_")).toDF().write
        .option("path", s"$work/occurrences_sd_2")
        .format("json")
        .saveAsTable("variant.occurrences_sd_2")

      writeSession.sql("drop table if exists variant.occurrences_sd_3")
      Seq(Occurrence(6, "A"), Occurrence(10, "_PUBLIC_")).toDF().write
        .option("path", s"$work/occurrences_sd_3")
        .format("json")
        .saveAsTable("variant.occurrences_sd_3")

      writeSession.sql("drop table if exists variant.occurrences_sd_4")
      Seq(Occurrence(7, "A"), Occurrence(8, "B")).toDF().write
        .option("path", s"$work/occurrences_sd_4")
        .format("json")
        .saveAsTable("variant.occurrences_sd_4")
      writeSession.stop()
      //Workaround to remove lock on derby db
      val path = Path("metastore_db/dbex.lck")
      path.delete()

      val readSession = getSparkSessionBuilder()
        .config("spark.sql.extensions", "org.kidsfirstdrc.dwh.spark.extension.DWHSparkSessionExtension")
        .config("spark.kf.dwh.acls", """{"SD_1":["A", "B"], "SD_2":["A", "D"], "SD_4": [],"SD_5": ["E"]}""")
        .getOrCreate()

      readSession.catalog.tableExists("occurrences") shouldBe true
      val expectedOccurrences = Seq(
        Occurrence(1, "A"),
        Occurrence(2, "B"),
        Occurrence(4, "A"),
        Occurrence(5, "D"),
        Occurrence(7, "A"),
        Occurrence(8, "B"),
        Occurrence(9, "_PUBLIC_"),
        Occurrence(10, "_PUBLIC_")

      )
      readSession.table("occurrences").as[Occurrence].collect() should contain theSameElementsAs expectedOccurrences
      readSession.sql("select * from occurrences").as[Occurrence].collect() should contain theSameElementsAs expectedOccurrences


    }
  }
  it should "not create occurrences view if acl not provided in sql conf" in {
    val writeSession = getSparkSessionBuilder().getOrCreate()
    import writeSession.implicits._
    writeSession.sql("create database if not exists variant")
    writeSession.sql("create database if not exists variant_live")
    withOutputFolder("work") { work =>

      writeSession.sql("drop table if exists variant.occurrences_sd_1")
      Seq(Occurrence(1, "A"), Occurrence(2, "B"), Occurrence(3, "C")).toDF().write
        .option("path", s"$work/occurrences_sd_1")
        .format("json")
        .saveAsTable("variant.occurrences_sd_1")
      writeSession.sql("drop table if exists variant.occurrences_sd_2")
      Seq(Occurrence(4, "A"), Occurrence(5, "D")).toDF().write
        .option("path", s"$work/occurrences_sd_2")
        .format("json")
        .saveAsTable("variant.occurrences_sd_2")
      writeSession.sql("drop table if exists variant.occurrences_sd_3")
      Seq(Occurrence(6, "A")).toDF().write
        .option("path", s"$work/occurrences_sd_3")
        .format("json")
        .saveAsTable("variant.occurrences_sd_3")
      writeSession.stop()
      //Workaround to remove lock on derby db
      val path = Path("metastore_db/dbex.lck")
      path.delete()

      val readSession = getSparkSessionBuilder()
        .config("spark.sql.extensions", "org.kidsfirstdrc.dwh.spark.extension.DWHSparkSessionExtension")
        .getOrCreate()

      readSession.catalog.tableExists("occurrences") shouldBe false


    }
  }

  it should "use variant_live db" in {
    val writeSession = getSparkSessionBuilder().getOrCreate()
    import writeSession.implicits._
    writeSession.sql("create database if not exists variant")
    writeSession.sql("create database if not exists variant_live")
    withOutputFolder("work") { work =>

      writeSession.sql("drop table if exists variant_live.test_table")
      Seq(Occurrence(1, "A"), Occurrence(2, "B")).toDF().write
        .option("path", s"$work/test_table")
        .format("json")
        .saveAsTable("variant_live.test_table")
      writeSession.stop()
      //Workaround to remove lock on derby db
      val path = Path("metastore_db/dbex.lck")
      path.delete()

      val readSession = getSparkSessionBuilder()
        .config("spark.sql.extensions", "org.kidsfirstdrc.dwh.spark.extension.DWHSparkSessionExtension")
        .getOrCreate()

      readSession.table("test_table").as[Occurrence].collect() should contain theSameElementsAs Seq(
        Occurrence(1, "A"),
        Occurrence(2, "B")
      )

    }
  }

  it should "expose a saved_sets  table" in {
    val writeSession = getSparkSessionBuilder().getOrCreate()
    import writeSession.implicits._
    writeSession.sql("create database if not exists variant")
    writeSession.sql("create database if not exists variant_live")
    withOutputFolder("work") { work =>

      writeSession.sql("drop table if exists variant_live.test_table")
      Seq(SavedSet("1", "A"), SavedSet("2", "B")).toDS().write
        .json(s"$work/test_saved_sets")
      writeSession.stop()
      //Workaround to remove lock on derby db
      val path = Path("metastore_db/dbex.lck")
      path.delete()

      val readSession = getSparkSessionBuilder()
        .config("spark.sql.extensions", "org.kidsfirstdrc.dwh.spark.extension.DWHSparkSessionExtension")
        .config("spark.kf.dwh.saved_sets", s"$work/test_saved_sets")
        .getOrCreate()

      readSession.table("saved_sets").as[SavedSet].collect() should contain theSameElementsAs Seq(
        SavedSet("1", "A"),
        SavedSet("2", "B")
      )

    }
  }
}
