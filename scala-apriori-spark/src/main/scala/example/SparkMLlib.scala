package example

import java.io.File

import example.data.{DatabaseInfo, Item, ItemSet}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SparkSession

import scala.io.StdIn

/**
  * Created by Vaclav Zeman on 2. 11. 2017.
  */
object SparkMLlib {

  def main(args: Array[String]): Unit = {

    val minSupport = 0.01

    System.setProperty("hadoop.home.dir", new File("hadoop").getAbsolutePath)
    val spark = SparkSession.builder().appName("apriori").master("local[*]").getOrCreate()

    /**
      * Start count mining time
      */
    val startTime = System.currentTimeMillis()

    val database = spark.read.format("csv").option("header", "true").load("KO_Bank_all.csv").rdd.repartition(spark.sparkContext.defaultParallelism).map { row =>
      row.getValuesMap[String](row.schema.fieldNames).iterator.map(item => Item(item._1, item._2)).toArray
    }.persist()
    implicit val databaseSize: DatabaseInfo = DatabaseInfo(database.count().toInt)

    val frequentItemsets = new FPGrowth().setMinSupport(minSupport).run(database).freqItemsets.map(x => ItemSet(x.freq.toInt, x.items: _*)).collect()
    frequentItemsets.foreach(println)

    println("Number of frequent itemsets: " + frequentItemsets.length)
    println(s"Mining time: ${(System.currentTimeMillis() - startTime) / 1000}s")

    println("Press enter to exit...")
    StdIn.readLine()

  }

}
