package comparison

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.matching.Regex

object Encoding {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Encoding").master("local[*]").getOrCreate()
    val input = spark.read.option("header", "true").csv("/Users/agupta/Desktop/cdc/logical_uri")

    spark.sparkContext.setLogLevel("ERROR")

    def findActualDiff(df: DataFrame, clm: Seq[String]): DataFrame = {

      df.as("df").join(
          df.as("df1"),
          clm.map(x => col(s"df.new_$x") === col(s"df1.new_$x")).reduce(_ && _)
        )
        .where(df.columns.map(x => col(s"df.$x") === col(s"df1.$x")).reduce(_ && _))
        .select(df.columns.map(x => col(s"df.$x")): _*)
        .distinct()
    }

    def newColumns(df: DataFrame, clm: Seq[String]): DataFrame =
      clm
        .foldLeft(df) { (buf, colm) =>
          buf.withColumn(s"new_$colm", extract(col(colm)))
        }
        .drop(clm: _*)

    def extract: UserDefinedFunction = udf { (x: String) =>

      def transform(x: String): String = {
        val pattern: Regex = "([\\d\\w\\s/-_:()$#,.!@%&^*+=|{}\\[\\]<>?;'\"\\\\~]+)".r
        val y = pattern.findAllMatchIn(x)
        y.map(
          k =>
            k.matched
              .replace("\\r\\n", "")
              .replace("\r\n", "")
              .replace("\r", "")
              .replace("\\r", "")
              .replace("\\n", "")
              .replace("\n", "")
              .replace("\\t", "")
              .replace("\t", "")
              .replaceAll("\\\\+", raw"\\")
              .replace(" ", "")
        )
          .mkString(" ")
          .trim
      }

      Option(x).map(transform)
    }

    def report(df: DataFrame, clm: Seq[String]): DataFrame = findActualDiff(newColumns(df, clm), clm)

    input.show(10, false)
    report(input, Seq("logical_uri")).show(10, false)
  }
}
