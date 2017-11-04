package example

import java.io.{File, FilenameFilter}

import example.data.{DatabaseInfo, Item, ItemSet, Transaction}
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by Vaclav Zeman on 2. 11. 2017.
  */
object SparkApriori {

  def main(args: Array[String]): Unit = {

    val minSupport = 0.01

    System.setProperty("hadoop.home.dir", new File("hadoop").getAbsolutePath)
    val spark = SparkSession.builder().appName("apriori").master("local[*]").getOrCreate()

    val database = spark.read.format("csv").option("header", "true").load("KO_Bank_all.csv").rdd.zipWithIndex().map { case (row, id) =>
      Transaction(id.toInt, row.getValuesMap[String](row.schema.fieldNames).iterator.map(item => Item(item._1, item._2)).toSet)
    }.persist()
    implicit val databaseSize: DatabaseInfo = DatabaseInfo(database.count().toInt)

    @scala.annotation.tailrec
    def mine(database: RDD[Transaction], itemsetLength: Int, totalNumOfItemsets: Long): Long = {
      def enumItemsets(tx: Transaction) = tx.items.subsets(itemsetLength)

      val frequentItemsets = database.flatMap(tx => enumItemsets(tx).map(_ -> 1)).reduceByKey(_ + _).map(x => ItemSet(x._2, x._1)).filter(_.relativeSupport >= minSupport).persist()
      frequentItemsets.saveAsTextFile(s"result-$itemsetLength")
      val numOfItemsets = frequentItemsets.count()
      val newDatabase = frequentItemsets.map(_.items -> true).join(database.flatMap(tx => enumItemsets(tx).map(_ -> tx.id))).map(x => x._2._2 -> x._1).reduceByKey(_ ++ _).map(x => Transaction(x._1, x._2)).persist()
      database.unpersist(false)
      frequentItemsets.unpersist(false)
      if (!newDatabase.isEmpty()) {
        mine(newDatabase, itemsetLength + 1, totalNumOfItemsets + numOfItemsets)
      } else {
        totalNumOfItemsets + numOfItemsets
      }
    }

    val numberOfItemsets = mine(database, 1, 0)

    val resultDirs = new File("./").listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = name.matches("result-\\d+")
    })

    for {
      dir <- resultDirs if dir.isDirectory
      file <- dir.listFiles(new FilenameFilter {
        def accept(dir: File, name: String): Boolean = name.matches("part-\\d+")
      }) if file.isFile
    } {
      val source = Source.fromFile(file)
      try {
        source.getLines().foreach(println)
      } finally {
        source.close()
      }
    }

    resultDirs.foreach(FileUtils.deleteDirectory)

    println("Number of frequent itemsets: " + numberOfItemsets)

  }

}
