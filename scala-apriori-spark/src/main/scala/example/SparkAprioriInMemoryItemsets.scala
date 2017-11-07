package example

import java.io.{File, FilenameFilter}

import example.data._
import org.apache.commons.io.FileUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.{Source, StdIn}

/**
  * Created by Vaclav Zeman on 2. 11. 2017.
  */
object SparkAprioriInMemoryItemsets {

  def main(args: Array[String]): Unit = {

    val minSupport = 0.01

    System.setProperty("hadoop.home.dir", new File("hadoop").getAbsolutePath)
    val spark = SparkSession.builder().appName("apriori").master("local[*]").getOrCreate()

    /**
      * Start count mining time
      */
    val startTime = System.currentTimeMillis()

    val database = spark.read.format("csv").option("header", "true").load("KO_Bank_all.csv").rdd.repartition(spark.sparkContext.defaultParallelism).zipWithIndex().map { case (row, id) =>
      Transaction(id.toInt, row.getValuesMap[String](row.schema.fieldNames).iterator.map(item => Item(item._1, item._2)).toSet)
    }.persist()
    implicit val databaseSize: DatabaseInfo = DatabaseInfo(database.count().toInt)
    val items = {
      val items = database
        .flatMap(_.items.map(_ -> 1))
        .reduceByKey(_ + _)
        .map(x => ItemSet(x._2, x._1))
        .filter(_.relativeSupport >= minSupport)
        .persist()
      items.saveAsTextFile(s"result-1")
      val itemsMap = items.zipWithIndex().collect().iterator.map(x => x._1.items.head -> x._2.toInt).toMap
      items.unpersist(false)
      spark.sparkContext.broadcast(itemsMap)
    }

    def expandItemset(itemsets: RDD[OrderedItemSet]): Array[OrderedItemSet] = itemsets.flatMap { itemset =>
      implicit val itemsMap: Map[Item, Int] = items.value
      itemsMap.keysIterator.flatMap(itemset + _)
    }.collect()

    @scala.annotation.tailrec
    def mine(itemsets: Broadcast[Seq[OrderedItemSet]], itemsetLength: Int): Unit = {
      val frequentItemsets = database.flatMap { tx =>
        itemsets.value.iterator.map(x => if (x.items.subsetOf(tx.items)) x -> 1 else x -> 0)
      }.reduceByKey(_ + _).map(x => x._1.withSupport(x._2)).filter(_.relativeSupport >= minSupport).persist()
      if (!frequentItemsets.isEmpty()) {
        frequentItemsets.saveAsTextFile(s"result-$itemsetLength")
        val newItemsets = expandItemset(frequentItemsets)
        frequentItemsets.unpersist(false)
        itemsets.unpersist()
        mine(spark.sparkContext.broadcast(newItemsets), itemsetLength + 1)
      }
    }

    mine(spark.sparkContext.broadcast(expandItemset(spark.sparkContext.parallelize(items.value.keysIterator.map(OrderedItemSet(0, _)).toSeq))), 2)

    val resultDirs = new File("./").listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = name.matches("result-\\d+")
    })

    var counter = 0

    for {
      dir <- resultDirs if dir.isDirectory
      file <- dir.listFiles(new FilenameFilter {
        def accept(dir: File, name: String): Boolean = name.matches("part-\\d+")
      }) if file.isFile
    } {
      val source = Source.fromFile(file)
      try {
        source.getLines().foreach { line =>
          counter += 1
          println(line)
        }
      } finally {
        source.close()
      }
    }

    resultDirs.foreach(FileUtils.deleteDirectory)

    println("Number of frequent itemsets: " + counter)
    println(s"Mining time: ${(System.currentTimeMillis() - startTime) / 1000}s")

    println("Press enter to exit...")
    StdIn.readLine()

  }

}
