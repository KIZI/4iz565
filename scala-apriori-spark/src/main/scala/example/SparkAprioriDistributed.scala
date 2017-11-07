package example

import java.io.{File, FilenameFilter}

import example.data.{DatabaseInfo, Item, ItemSet, Transaction}
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.{Source, StdIn}

/**
  * Created by Vaclav Zeman on 2. 11. 2017.
  */
object SparkAprioriDistributed {

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

    @scala.annotation.tailrec
    def mine(database: RDD[Transaction], itemsetLength: Int): Unit = {
      def enumItemsets(tx: Transaction) = tx.items.subsets(itemsetLength)

      val frequentItemsets = database.flatMap(tx => enumItemsets(tx).map(_ -> 1)).reduceByKey(_ + _).map(x => ItemSet(x._2, x._1)).filter(_.relativeSupport >= minSupport).persist()
      frequentItemsets.saveAsTextFile(s"result-$itemsetLength")
      val newDatabase = frequentItemsets.map(_.items -> true).join(database.flatMap(tx => enumItemsets(tx).map(_ -> tx.id))).map(x => x._2._2 -> x._1).reduceByKey(_ ++ _).map(x => Transaction(x._1, x._2)).persist()
      if (!newDatabase.isEmpty()) {
        database.unpersist(false)
        frequentItemsets.unpersist(false)
        mine(newDatabase, itemsetLength + 1)
      } else {
        database.unpersist(false)
        frequentItemsets.unpersist(false)
      }
    }

    mine(database, 1)

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
