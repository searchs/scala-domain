package com.ohprice.nlytics.utils

import java.util.Locale


import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

trait SourceReader {


  def readSourceData(sourceInputLocation: String)(implicit spark: SparkSession): DataFrame = {
    val fileType = FilenameUtils.getExtension(sourceInputLocation)

    fileType.toLowerCase(Locale.ROOT) match {
      case "csv" =>
        spark.read.format("csv")
          .option("header", "true")
          .option("delimiter", ",")
          .load(sourceInputLocation)
      case "bsv" =>
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", "|")
        .load(sourceInputLocation)
      case "parq" =>
        spark.read.parquet(sourceInputLocation)
      case "delta" =>
        spark.read.format("delta").load(sourceInputLocation)
      case _ =>
        throw new RuntimeException("File type is not recognised : " + sourceInputLocation)
    }
  }

  def readSourceData(basePath: String,
                     sourceInputLocation: Array[String])(implicit spark: SparkSession): DataFrame = {
    val fileType = FilenameUtils.getExtension(basePath)

    fileType.toLowerCase(Locale.ROOT) match {
      case "csv" =>
        spark.read.format("csv")
          .option("header", "true")
          .option("delimiter", ",")
          .option("basePath", basePath)
          .load(sourceInputLocation: _*)
      case "bsv" =>
        spark.read.format("csv")
          .option("header", "true")
          .option("delimiter", "|")
          .option("basePath", basePath)
          .load(sourceInputLocation: _*)
      case "parq" =>
        spark.read
          .option("basePath", basePath)
          .parquet(sourceInputLocation: _*)
      case "delta" =>
        spark.read.format("delta")
          .option("basePath", basePath)
          .load(sourceInputLocation: _*)
      case _ =>
        throw new RuntimeException("File type is not recognised : " + basePath)
    }
  }




}
