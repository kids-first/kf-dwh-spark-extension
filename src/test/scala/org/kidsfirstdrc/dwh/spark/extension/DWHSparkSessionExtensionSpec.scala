package org.kidsfirstdrc.dwh.spark.extension


import org.kidsfirstdrc.dwh.spark.extension.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Path

case class Occurence(id: Int, dbgap_consent_code: String)
case class SavedSet(id: String, name: String)

class DWHSparkSessionExtensionSpec extends AnyFlatSpec with WithSparkSession with Matchers {


  "use session extension" should "create occurences view" in {
    val writeSession = getSparkSessionBuilder().getOrCreate()
    import writeSession.implicits._
    writeSession.sql("create database if not exists variant")
    writeSession.sql("create database if not exists variant_live")
    withOutputFolder("work") { work =>

      writeSession.sql("drop table if exists variant.occurences_sd_1")
      Seq(Occurence(1, "A"), Occurence(2, "B"), Occurence(3, "C")).toDF().write
        .option("path", s"$work/occurences_sd_1")
        .format("json")
        .saveAsTable("variant.occurences_sd_1")
      writeSession.sql("drop table if exists variant.occurences_sd_2")
      Seq(Occurence(4, "A"), Occurence(5, "D")).toDF().write
        .option("path", s"$work/occurences_sd_2")
        .format("json")
        .saveAsTable("variant.occurences_sd_2")
      writeSession.sql("drop table if exists variant.occurences_sd_3")
      Seq(Occurence(6, "A")).toDF().write
        .option("path", s"$work/occurences_sd_3")
        .format("json")
        .saveAsTable("variant.occurences_sd_3")
      writeSession.stop()
      //Workaround to remove lock on derby db
      val path = Path("metastore_db/dbex.lck")
      path.delete()

      val readSession = getSparkSessionBuilder()
        .config("spark.sql.extensions", "org.kidsfirstdrc.dwh.spark.extension.DWHSparkSessionExtension")
        .config("kf.dwh.acls", """{"SD_1":["A", "B"], "SD_2":["A", "D"]}""")
        .getOrCreate()

      readSession.catalog.tableExists("occurences") shouldBe true
      readSession.table("occurences").as[Occurence].collect() should contain theSameElementsAs Seq(
        Occurence(1, "A"),
        Occurence(2, "B"),
        Occurence(4, "A"),
        Occurence(5, "D")
      )


      readSession.sql("select * from occurences").as[Occurence].collect() should contain theSameElementsAs Seq(
        Occurence(1, "A"),
        Occurence(2, "B"),
        Occurence(4, "A"),
        Occurence(5, "D")
      )


    }
  }
  it should "not create occurences view if acl not provided in sql conf" in {
    val writeSession = getSparkSessionBuilder().getOrCreate()
    import writeSession.implicits._
    writeSession.sql("create database if not exists variant")
    writeSession.sql("create database if not exists variant_live")
    withOutputFolder("work") { work =>

      writeSession.sql("drop table if exists variant.occurences_sd_1")
      Seq(Occurence(1, "A"), Occurence(2, "B"), Occurence(3, "C")).toDF().write
        .option("path", s"$work/occurences_sd_1")
        .format("json")
        .saveAsTable("variant.occurences_sd_1")
      writeSession.sql("drop table if exists variant.occurences_sd_2")
      Seq(Occurence(4, "A"), Occurence(5, "D")).toDF().write
        .option("path", s"$work/occurences_sd_2")
        .format("json")
        .saveAsTable("variant.occurences_sd_2")
      writeSession.sql("drop table if exists variant.occurences_sd_3")
      Seq(Occurence(6, "A")).toDF().write
        .option("path", s"$work/occurences_sd_3")
        .format("json")
        .saveAsTable("variant.occurences_sd_3")
      writeSession.stop()
      //Workaround to remove lock on derby db
      val path = Path("metastore_db/dbex.lck")
      path.delete()

      val readSession = getSparkSessionBuilder()
        .config("spark.sql.extensions", "org.kidsfirstdrc.dwh.spark.extension.DWHSparkSessionExtension")
        .getOrCreate()

      readSession.catalog.tableExists("occurences") shouldBe false


    }
  }

  it should "use variant_live db" in {
    val writeSession = getSparkSessionBuilder().getOrCreate()
    import writeSession.implicits._
    writeSession.sql("create database if not exists variant")
    writeSession.sql("create database if not exists variant_live")
    withOutputFolder("work") { work =>

      writeSession.sql("drop table if exists variant_live.test_table")
      Seq(Occurence(1, "A"), Occurence(2, "B")).toDF().write
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

      readSession.table("test_table").as[Occurence].collect() should contain theSameElementsAs Seq(
        Occurence(1, "A"),
        Occurence(2, "B")
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
        .config("kf.dwh.saved_sets", s"$work/test_saved_sets")
        .getOrCreate()

      readSession.table("saved_sets").as[SavedSet].collect() should contain theSameElementsAs Seq(
        SavedSet("1", "A"),
        SavedSet("2", "B")
      )

    }
  }
}
