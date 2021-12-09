import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

val spark = SparkSession.builder().appName("Validation").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

//val schema = products_description_schema
//val toRow = products_description_row

// ######
// Report 1: printTotalComparison(readDFs("s3://imvu-data4dev/mysql/priority-1/2021-11-22/master/tables/logical_uri_mapping/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/logical_uri_mapping", "20211123093710"))
// Report 2: printNumericColumnsComparison(readDFs("s3://imvu-data4dev/mysql/priority-1/2021-11-22/master/tables/logical_uri_mapping/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/logical_uri_mapping", "20211123093710"), 1, "id")
// Report 3: printNonNumericColumnsComparison(readDFs("s3://imvu-data4dev/mysql/priority-1/2021-11-25/master/tables/products/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/products", "20211123113050"), "s3://production-cdc-hudi/validation_result/logical_uri_mapping/2021-11-22/", "id")
// ######

val housekeepingColumns = Seq(
  "_hoodie_commit_time",
  "_hoodie_commit_seqno",
  "_hoodie_record_key",
  "_hoodie_partition_path",
  "_hoodie_file_name",
  "__op",
  "__table",
  "__ts_ms",
  "__source_ts_ms",
  "_hoodie_is_deleted"
)

// Utils
def t(table: String): DataFrame = spark.read.table(table)
def q(query: String): DataFrame = spark.sql(s"""${query.stripMargin}""")

def excludeColumn(in: DataFrame, columns: Seq[String], allColumns: Seq[String]): DataFrame =
  columns.foldLeft(in) { (df, clm) =>
    if (allColumns.contains(clm)) df.drop(clm)
    else df
  }

def getNumericColumnList(df: DataFrame): Seq[String] =
  df.schema
    .filter(
      x =>
        x.dataType.simpleString == "double"
          || x.dataType.simpleString == "float"
          || x.dataType.simpleString.contains("decimal")
    )
    .map(_.name)

def roundScientific(columnName: String): Column =
  col(columnName).cast(DecimalType(18, 2))

def roundNumericColumns(df: DataFrame): DataFrame = {
  val numericColumns = getNumericColumnList(df)
  numericColumns.foldLeft(df)((dff, clm) => dff.withColumn(clm, roundScientific(clm)))
}

def removeNumericColumns(df: DataFrame): DataFrame = {
  val numericColumns = getNumericColumnList(df)
  numericColumns.foldLeft(df)((dff, clm) => dff.drop(clm))
}

// Reader
case class DfDuo(df1: DataFrame, df2: DataFrame)

def readDFs(table1: String, table2: String, table2_end_time: String, schema: StructType, toRow: Array[String] => Row): DfDuo = {

  // Read Hive table
  //  val fullPath1 = table1.split("\\.")
  //  val (db1, tbl1) = (fullPath1(0), fullPath1(1))
  //  val df1 = spark.table(table1)
  //  val columns1 = df1.columns.toList

  val rdd: RDD[String] = spark.sparkContext.textFile(table1)
  val rows = rdd
    .map(_.split("\t", -1))
    .map(
      r =>
        r.map(
          v =>
            if (v == "\\N") null
            else v.replace("\\r\\n", "\r\n").replace("\\t", "\t").replace("\\\\", "\\")
        )
    )
    .map(toRow)
  val df1 = spark.createDataFrame(rows, schema)
  val columns1 = df1.columns.toList

  // Read Hudi table
  val df2 = spark.read.format("org.apache.hudi").option("as.of.instant", table2_end_time).load(table2)

  //  df1.persist(StorageLevel.MEMORY_AND_DISK)
  //  df2.persist(StorageLevel.MEMORY_AND_DISK)

  val columns2 = df2.columns.toList

  DfDuo(
    excludeColumn(df1, housekeepingColumns, columns1),
    excludeColumn(df2, housekeepingColumns, columns2)
  )
}

// Report 1
case class RoundNRemoveDf(roundDf: DataFrame, removeDf: DataFrame)

def compareTotals(df1: DataFrame, df2: DataFrame): RoundNRemoveDf = {

  val count1 = df1.count
  val count2 = df2.count
  val exceptAllDf = df1.exceptAll(df2)
  val exceptAllCount = exceptAllDf.count

  val exceptAllRoundedDf = roundNumericColumns(df1).exceptAll(roundNumericColumns(df2))
  val exceptAllCountRoundedDf = exceptAllRoundedDf.count

  val exceptAllNoNumericDf = removeNumericColumns(df1).exceptAll(removeNumericColumns(df2))
  val exceptAllCountNoNumericDf = exceptAllNoNumericDf.count

  println("\n#############################################################")
  println(s"df1.count: $count1 \t\t\t df2.count: $count2")
  println(s"df1.count - df2.count \t\t\t\t: ${count1 - count2}")

  println(s"df1.exceptAll(df2).count\t\t\t: $exceptAllCount")
  println(s"round(df1).exceptAll(round(df2))\t\t: $exceptAllCountRoundedDf")

  println(s"noNumeric(df1).exceptAll(noNumeric(df2))\t: $exceptAllCountNoNumericDf")
  println("###############################################################\n")

  RoundNRemoveDf(exceptAllRoundedDf, exceptAllNoNumericDf)
}

def printTotalComparison(dfDuo: DfDuo): RoundNRemoveDf = compareTotals(dfDuo.df1, dfDuo.df2)

// Report 2
case class DiffPlusVariance(diffs: DataFrame, diffsWithRound: DataFrame, variance: DataFrame)

def compareNumericColumns(tolerance: Double, lmt: Int)(
    origDf1: DataFrame,
    origDf2: DataFrame,
    resultingReportPath: Option[String],
    keyClms: String*
): DiffPlusVariance = {

  // Only leave numeric columns
  val numericColumns = getNumericColumnList(origDf1)
  val requiredClms = keyClms.toList ++ numericColumns
  val df1 = origDf1.select(requiredClms.head, requiredClms.tail: _*)
  val df2 = origDf2.select(requiredClms.head, requiredClms.tail: _*)

  val exceptAllDf = df1.exceptAll(df2)
  val exceptAllRoundedDf = roundNumericColumns(df1).exceptAll(roundNumericColumns(df2))

  // Join mismatching records into one df. To see columns side by side.
  val numericColumnsWithAliasDf2 =
    origDf2.columns.foldLeft(origDf2)((df, clm) => df.withColumnRenamed(clm, s"hudi_$clm"))
  import org.apache.spark.sql.functions._
  val joinColumns: Column = keyClms.map(x => col(x) <=> col(s"hudi_$x")).reduce(_ && _)
  val filteredColumns = exceptAllDf.columns.filterNot(x => keyClms.contains(x))
  val zippedNumericClms = filteredColumns.zip(numericColumns.map(x => s"hudi_$x"))
  val finalNumericColumns: List[String] = zippedNumericClms
    .foldLeft(List[String]()) { (buff, ls) =>
      ls._1 :: ls._2 :: buff
    }
    .reverse

  val finalColumns = (keyClms ++ finalNumericColumns).map(col)

  val joinedDf =
    exceptAllDf.join(numericColumnsWithAliasDf2, joinColumns).select(finalColumns: _*)
  joinedDf.cache()
  val joinedDfRounded = exceptAllRoundedDf
    .join(roundNumericColumns(numericColumnsWithAliasDf2), joinColumns)
    .select(finalColumns: _*)
  val joinedDfRoundedSample = joinedDfRounded.limit(lmt).orderBy(keyClms.head, keyClms.tail: _*)

  // This step is very imp, because the joinedDfRounded df if used in 2 places. One for join and after that show.
  // If it's not cached, it it evaluated both times and produces different result
  joinedDfRoundedSample.cache()

  // Display difference
  println("\n########### Compare df1 vs df2 mismatch only numeric columns ###########")
  println(s"joinedDf : ${joinedDf.count}")
  joinedDf.show(lmt, truncate = false)
  if (!joinedDfRoundedSample.isEmpty) {
    joinedDf
      .join(joinedDfRoundedSample, keyClms)
      .select(joinedDf.columns.map(x => joinedDf(x)): _*)
      .show(lmt, truncate = false)
  } else {
    joinedDf.show(lmt, truncate = false)
  }

  // Display difference rounded
  if (!joinedDfRoundedSample.isEmpty) {
    println(
      "\n########### Compare df1 vs df2 tables mismatch only numeric columns with rounding ###########"
    )
    println(s"joinedDfRounded : ${joinedDfRounded.count}")
    joinedDfRoundedSample.show(lmt, truncate = false)
  }

  // Calculate difference (|col1 - col2| > tolerance)
  val diffOfColumns: Seq[(Column, String)] =
    numericColumns.map(x => (roundScientific(x) - roundScientific(s"hudi_$x"), x))
  val varianceDf: DataFrame =
    diffOfColumns.foldLeft(joinedDf)((df, clm) => df.withColumn(s"diff_${clm._2}", clm._1))
  val varianceDfFinal =
    numericColumns.foldLeft(varianceDf)((df, clm) => df.drop(clm).drop(s"hudi_$clm"))

  val diffColumns: Array[String] = varianceDfFinal.columns.filter(_.contains("diff"))
  val unTolerable = diffColumns.foldLeft(varianceDfFinal)(
    (_, columnName) => varianceDfFinal.filter(abs(col(columnName)) > tolerance)
  )
  unTolerable.cache()

  // Display unTolerable difference
  print(
    s"\n########### Compare df1 vs df2 tables mismatch only numeric columns with tolerance ###########\n"
  )
  println(s"Above tolerance limit: ${unTolerable.count}\n")
  unTolerable
    .join(joinedDfRoundedSample, keyClms)
    .select(unTolerable.columns.map(x => unTolerable(x)): _*)
    .show(lmt, truncate = false)
  unTolerable.show(lmt, truncate = false)

  resultingReportPath.foreach { x =>
    joinedDf
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(x + "/numerics/")
  }

  joinedDf.unpersist()
  joinedDfRoundedSample.unpersist()
  DiffPlusVariance(joinedDf, joinedDf, varianceDfFinal)
}

def printNumericColumnsComparison(
    dfDuo: DfDuo,
    tolerance: Double,
    resultingReportPath: Option[String],
    keyClms: String*
): DiffPlusVariance =
  compareNumericColumns(tolerance, 10)(dfDuo.df1, dfDuo.df2, resultingReportPath, keyClms: _*)

// Report 3
def compareNonNumericColumns(lmt: Int)(
    fullDf1: DataFrame,
    fullDf2: DataFrame,
    resultingReportPath: Option[String],
    keyClms: String*
): DataFrame = {

  assert(
    keyClms.nonEmpty,
    "Please provide the key columns that are unique identifiers per row in both tables"
  )

  val df1 = removeNumericColumns(fullDf1)
  val df2 = removeNumericColumns(fullDf2)

  val exceptAllDf = df1.exceptAll(df2)

  val df2WithAlias =
    df2.columns.foldLeft(df2)((df, clm) => df.withColumnRenamed(clm, s"hudi_$clm"))
  val joinColumns: Column = keyClms.map(x => col(x) <=> col(s"hudi_$x")).reduce(_ && _)
  val filteredColumns = exceptAllDf.columns.filterNot(x => keyClms.contains(x))
  val zippedAllClms = filteredColumns.zip(filteredColumns.map(x => s"hudi_$x"))
  val flattenedClms: List[String] = zippedAllClms.foldLeft(List[String]()) { (buff, ls) =>
    ls._1 :: ls._2 :: buff
  }
  val finalColumns = (keyClms ++ flattenedClms).map(col)

  val joinedDf = exceptAllDf.join(df2WithAlias, joinColumns).select(finalColumns: _*)
  joinedDf.cache()

  // Remove matching columns. Just to see a sample of what exactly is different.
  val nonMatchingColumnFlagDf: DataFrame = filteredColumns.foldLeft(joinedDf)(
    (df, clm) => df.withColumn(s"comp_$clm", when(col(clm) <=> col(s"hudi_$clm"), 0).otherwise(1))
  )
  val flagColumns = nonMatchingColumnFlagDf.columns.filter(_.contains("comp_"))
  val nonMatchingColumnFlagSumDf = nonMatchingColumnFlagDf
    .select(flagColumns.head, flagColumns.tail: _*)
    .agg(flagColumns.map(x => (x, "sum")).toMap)
  val removeColumns: Array[String] = flagColumns
    .foldLeft(nonMatchingColumnFlagSumDf)((df, clm) => {
      val dfWithRenClm = df.withColumnRenamed(s"sum($clm)", s"$clm")
      if (dfWithRenClm.filter(col(clm) === 0).isEmpty)
        dfWithRenClm.drop(s"$clm")
      else dfWithRenClm
    })
    .columns
    .map(_.substring(5))
  val finalCompDF = removeColumns.foldLeft(joinedDf)((df, clm) => df.drop(clm, s"hudi_$clm"))
  finalCompDF.cache()

  println("\n########### Compare df1 vs df2 tables mismatch non numeric columns ###########")
  println(s"Total mismatching: ${finalCompDF.count}\n")
  finalCompDF.show(lmt, truncate = false)

  finalCompDF.columns
    .filterNot(clm => clm contains "hudi")
    .filterNot(clm => keyClms.contains(clm))
    .foreach { clm =>
      val diffPerColumnDf =
        finalCompDF.select(clm, s"hudi_$clm", keyClms.head).filter(col(clm) =!= col(s"hudi_$clm"))
      println("Number of mismatching rows in column " + clm)
      println(diffPerColumnDf.count)
      diffPerColumnDf.show(10)
      resultingReportPath.foreach(
        x => diffPerColumnDf.coalesce(1).write.option("header", "true").mode("overwrite").csv(x + "/" + clm)
      )
    }

  resultingReportPath match {
    case Some(value) =>
      finalCompDF.coalesce(1).write.option("header", "true").mode("overwrite").csv(value)
      exceptAllDf.coalesce(1).write.option("header", "true").mode("overwrite").csv(value + "/missing/")
    case None =>
  }

  joinedDf.unpersist()
  finalCompDF.unpersist()
  df1.unpersist()
  df2.unpersist()

  finalCompDF
}

def printNonNumericColumnsComparison(
    dfDuo: DfDuo,
    resultingReportPath: Option[String],
    keyClms: String*
): DataFrame =
  compareNonNumericColumns(10)(dfDuo.df1, dfDuo.df2, resultingReportPath, keyClms: _*)