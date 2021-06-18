/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf.ssb
import scala.sys.process._

import com.databricks.spark.sql.perf.{Benchmark, BlockingLineStream, DataGenerator, Table, Tables}
import com.databricks.spark.sql.perf.ExecutionMode.CollectResults
import org.apache.commons.io.IOUtils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class DBGEN(dbgenDir: String, params: Seq[String]) extends DataGenerator {
  val dbgen = s"$dbgenDir/dbgen"
  def generate(sparkContext: SparkContext,name: String, partitions: Int, scaleFactor: String) = {
    val smallTables = Seq("nation", "region")
    val numPartitions = if (partitions > 1 && !smallTables.contains(name)) partitions else 1
    val generatedData = {
      sparkContext.parallelize(1 to numPartitions, numPartitions).flatMap { i =>
        val localToolsDir = if (new java.io.File(dbgen).exists) {
          dbgenDir
        } else if (new java.io.File(s"/$dbgenDir").exists) {
          s"/$dbgenDir"
        } else {
          sys.error(s"Could not find dbgen at $dbgen or /$dbgenDir. Run install")
        }
        val parallel = if (numPartitions > 1) s"-C $partitions -S $i" else ""
        val shortTableNames = Map(
          "customer" -> "c",
          "p_lineorders" -> "l",
          "part" -> "p",
          "supplier" -> "s",
          "dates" -> "d"
        )
        val paramsString = params.mkString(" ")
        val commands = Seq(
          "bash", "-c",
          s"cd $localToolsDir && ./dbgen -q $paramsString -T ${shortTableNames(name)} -s $scaleFactor $parallel")
        println(commands)
        BlockingLineStream(commands)
      }.repartition(numPartitions)
    }

    generatedData.setName(s"$name, sf=$scaleFactor, strings")
    generatedData
  }
}

class SSBTables(
    sqlContext: SQLContext,
    dbgenDir: String,
    scaleFactor: String,
    useDoubleForDecimal: Boolean = false,
    useStringForDate: Boolean = false,
    generatorParams: Seq[String] = Nil)
    extends Tables(sqlContext, scaleFactor, useDoubleForDecimal, useStringForDate) {
  import sqlContext.implicits._

  val dataGenerator = new DBGEN(dbgenDir, generatorParams)

  val tables = Seq(
    Table("part",
      partitionColumns = "p_brand" :: Nil,
      'p_partkey.long,
      'p_name.string,
      'p_mfgr.string,
      'p_category.string,
      'p_brand.string,
      'p_color.string,
      'p_type.string,
      'p_size.int,
      'p_container.string
    ),
    Table("supplier",
      partitionColumns = Nil,
      's_suppkey.long,
      's_name.string,
      's_address.string,
      's_city.string,
      's_nation.string,
      's_region.string,
      's_phone.string
    ),
    Table("customer",
      partitionColumns = "c_mktsegment" :: Nil,
      'c_custkey.long,
      'c_name.string,
      'c_address.string,
      'c_city.string,
      'c_nation.string,
      'c_region.string,
      'c_phone.string,
      'c_mktsegment.string
    ),
    Table("dates",
      partitionColumns = Nil,
      'd_datekey.long,
      'd_date.string,
      'd_dayofweek.string,
      'd_month.string,
      'd_year.string,
      'd_yearmonthnum.string,
      'd_yearmonth.string,
      'd_daynuminweek.long,
      'd_daynuminmonth.long,
      'd_daynuminyear.long,
      'd_monthnuminyear.long,
      'd_weeknuminyear.long,
      'd_sellingseason.string,
      'd_lastdayinweekfl.long,
      'd_lastdayinmonthfl.long,
      'd_holidayfl.long,
      'd_weekdayfl.long
    ),
    Table("p_lineorders",
      partitionColumns = "lo_orderdate" :: Nil,
      'lo_orderkey.long,
      'lo_linenumber.long,
      'lo_custkey.long,
      'lo_partkey.int,
      'lo_suppkey.int,
      'lo_orderdate.int,
      'lo_orderpriority.string,
      'lo_shippriority.int,
      'lo_quantity.long,
      'lo_extendedprice.long,
      'lo_ordtotalprice.long,
      'lo_discount.long,
      'lo_revenue.long,
      'lo_supplycost.long,
      'lo_tax.long,
      'lo_commitdate.int,
      'lo_shipmode.string
    )
  ).map(_.convertTypes())
}

class SSB(@transient sqlContext: SQLContext)
  extends Benchmark(sqlContext) {

  val queries = (1 to 13).map { q =>
    val queryContent: String = IOUtils.toString(
      getClass().getClassLoader().getResourceAsStream(s"ssb/queries/$q.sql"))
    Query(s"Q$q", queryContent, description = "SSB Query",
      executionMode = CollectResults)
  }
  val queriesMap = queries.map(q => q.name.split("-").get(0) -> q).toMap
}
