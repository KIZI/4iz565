package example

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import example.NonParallelApriori.minSupport
import example.data.{ItemSet, Database}

/**
  * Created by Vaclav Zeman on 19. 10. 2017.
  */
object ParallelCollectionApriori {

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
      * This operation is processed in parallel!
      * See the ".par" method!
      */
    val itemsSets = transactions.items.par.flatMap(ItemSet().expandWith).filter(_.relativeSupport >= minSupport)
    val items = itemsSets.flatMap(_.items).seq

    /**
      * Recursive function which expands an itemset with a new item
      * The new itemset must have relative support greater or equal than min support
      * Finally this function enumerates all possible itemsets that satisfy the min support threshold
      */
    def enumItemSets(itemSet: ItemSet): Iterator[ItemSet] = {
      Iterator(itemSet) ++ items.iterator.flatMap(itemSet.expandWith).filter(_.relativeSupport >= minSupport).flatMap(enumItemSets)
    }

    /**
      * "itemsSets" value is a parallel set which contains all frequent items
      * For each this item we recursively expand it with other items
      * Expansion function "enumItemSets" is calling in parallel for each item!
      * The parallelism level is counted automaticaly by number of cores
      */
    itemsSets.foreach { itemset =>
      enumItemSets(itemset).foreach { itemset =>
        println(itemset)
        counter.incrementAndGet()
      }
    }

    println(s"Number of rules: ${counter.get()}")
    println(s"Mining time: ${(System.currentTimeMillis() - startTime) / 1000}s")
  }

}
