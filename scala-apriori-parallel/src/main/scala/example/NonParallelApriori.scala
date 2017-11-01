package example

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import example.data.{ItemSet, Database}

/**
  * Created by Vaclav Zeman on 31. 10. 2017.
  */
object NonParallelApriori {

  /**
    * Min support threshold for apriori pruning
    */
  val minSupport = 0.01

  val counter = new AtomicInteger(0)

  def main(args: Array[String]): Unit = {
    /**
      * Start count mining time
      */
    val startTime = System.currentTimeMillis()

    /**
      * Load all transactions from a dataset
      */
    implicit val transactions: Database = Database.fromCsv(new File("KO_Bank_all.csv"))

    /**
      * Get all items from transactions which have relative support greater or equal than min support
      */
    val items = transactions.items.filter(item => ItemSet().expandWith(item).exists(_.relativeSupport >= minSupport))

    /**
      * Recursive function which expands an itemset with a new item
      * The new itemset must have relative support greater or equal than min support
      * Finally this function enumerates all possible itemsets that satisfy the min support threshold
      */
    def enumItemSets(itemSet: ItemSet): Iterator[ItemSet] = {
      Iterator(itemSet) ++ items.iterator.flatMap(itemSet.expandWith).filter(_.relativeSupport >= minSupport).flatMap(enumItemSets)
    }

    /**
      * Print all enumerated itemsets and count their quantity
      */
    enumItemSets(ItemSet()).drop(1).foreach { itemset =>
      println(itemset)
      counter.incrementAndGet()
    }

    println(s"Number of rules: ${counter.get()}")
    println(s"Mining time: ${(System.currentTimeMillis() - startTime) / 1000}s")
  }

}
