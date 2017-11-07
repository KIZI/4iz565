package example

import java.io.File

import example.data._
import org.apache.spark.sql.SparkSession

import scala.io.StdIn

/**
  * Created by Vaclav Zeman on 5. 11. 2017.
  */
object SparkAprioriInMemoryDatabase1 {

  def main(args: Array[String]): Unit = {

    val minSupport = 0.01

    System.setProperty("hadoop.home.dir", new File("hadoop").getAbsolutePath)
    val spark = SparkSession.builder().appName("apriori").master("local[*]").getOrCreate()

    /**
      * Start count mining time
      */
    val startTime = System.currentTimeMillis()

    val rddDatabase = spark.read.format("csv").option("header", "true").load("KO_Bank_all.csv").rdd.repartition(spark.sparkContext.defaultParallelism).zipWithIndex().map { case (row, id) =>
      Transaction(id.toInt, row.getValuesMap[String](row.schema.fieldNames).iterator.map(item => Item(item._1, item._2)).toSet)
    }.persist()

    val database = spark.sparkContext.broadcast(rddDatabase.collect())
    implicit val databaseSize: DatabaseInfo = DatabaseInfo(database.value.length)
    val initItemsets = rddDatabase
      .flatMap(_.items.map(_ -> 1))
      .reduceByKey(_ + _)
      .map(x => OrderedItemSet(x._2, x._1))
      .filter(_.relativeSupport >= minSupport)
      .persist()
    val items = spark.sparkContext.broadcast(
      initItemsets
        .zipWithIndex()
        .collect()
        .iterator
        .map(x => x._1.items.head -> x._2.toInt)
        .toMap
    )

    var counter = 0

    initItemsets.flatMap { item =>
      implicit val itemsMap: Map[Item, Int] = items.value

      def enumItemsets(itemset: OrderedItemSet): Iterator[OrderedItemSet] = {
        Iterator(itemset) ++ itemsMap.keysIterator.flatMap(itemset + _).map { itemset =>
          val support = database.value.count(tx => itemset.items.subsetOf(tx.items))
          itemset.withSupport(support)
        }.filter(_.relativeSupport >= minSupport).flatMap(enumItemsets)
      }

      enumItemsets(item)
    }.collect().foreach { itemset =>
      counter += 1
      println(itemset)
    }

    println("Number of frequent itemsets: " + counter)
    println(s"Mining time: ${(System.currentTimeMillis() - startTime) / 1000}s")

    println("Press enter to exit...")
    StdIn.readLine()

  }

}
