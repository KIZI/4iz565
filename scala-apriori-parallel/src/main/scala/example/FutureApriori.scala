package example

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import example.NonParallelApriori.minSupport
import example.data.{ItemSet, Database}
import example.utils.CollectionExtensions._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Created by Vaclav Zeman on 31. 10. 2017.
  */
object FutureApriori {

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
      * Create an execution context which is need for Future operations
      * Count the number of availaible cores
      */
    implicit val ec: ExecutionContext = ExecutionContext.global
    val cores = Runtime.getRuntime.availableProcessors()

    /**
      * Get all items from transactions which have relative support greater or equal than min support
      * This operation is processed in parallel!
      * First we split all items to "p" groups where "p" is the number of cores
      * For each this group we asynchronously filter all items which satisfy the min support threshold
      * The Future block creates a new thread!
      * Finally we have created "p" threads represented by Future objects
      */
    val items = transactions.items.toGroups(cores).map { items =>
      Future {
        items.flatMap(ItemSet().expandWith).filter(_.relativeSupport >= minSupport)
      }
    }.toList

    /**
      * We asynchronously reduce all filtered items from all threads
      * The "map" function is fired as soon as all Future objects are successfully counted and all items are reduced
      * Then we use filtered items for enumeration all frequent itemsets - see the inner block!
      */
    val futureResult = Future.reduceLeft(items)(_ ++ _).map { itemsSets =>
      val items = itemsSets.flatMap(_.items)

      /**
        * Recursive function which expands an itemset with a new item
        * The new itemset must have relative support greater or equal than min support
        * Finally this function enumerates all possible itemsets that satisfy the min support threshold
        */
      def enumItemSets(itemSet: ItemSet): Iterator[ItemSet] = {
        Iterator(itemSet) ++ items.iterator.flatMap(itemSet.expandWith).filter(_.relativeSupport >= minSupport).flatMap(enumItemSets)
      }

      /**
        * First we split all frequent items to "p" groups where "p" is the number of cores
        * For each this group we recursively and asynchronously expand all items in the group
        * The Future block creates a new thread!
        * Finally we have created "p" threads represented by Future objects that concurrently enumerate all itemsets
        */
      itemsSets.toGroups(cores).map { itemsSets =>
        Future {
          itemsSets.foreach { itemset =>
            enumItemSets(itemset).foreach { itemset =>
              println(itemset)
              counter.incrementAndGet()
            }
          }
        }
      }.toList
    }

    /**
      * The "futureResult" value is a Future object which contains a list of Future objects
      * We aggregate all futures within this value to the one Future object which indicates that all future objects have been completed
      * In this place we are waiting for completion of all Future objects
      */
    Await.result(futureResult.flatMap(Future.sequence(_)), Duration.Inf)

    println(s"Number of frequent itemsets: ${counter.get()}")
    println(s"Mining time: ${(System.currentTimeMillis() - startTime) / 1000}s")
  }

}
