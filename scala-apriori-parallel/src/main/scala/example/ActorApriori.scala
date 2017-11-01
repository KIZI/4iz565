package example

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing.{Broadcast, RoundRobinRoutingLogic, Router}
import example.ActorApriori.AprioriActor._
import example.NonParallelApriori.minSupport
import example.data.{Item, ItemSet, Transactions}
import example.utils.CollectionExtensions._

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent._

/**
  * Created by Vaclav Zeman on 31. 10. 2017.
  */
object ActorApriori {

  val counter = new AtomicInteger(0)

  /**
    * Apriori actor which is able to:
    *  - register other actor (worker),
    *  - add itemsets to the queue,
    *  - expand itemsets,
    *  - ask other actors for work,
    *  - divide itemsets queue and forward some part to another actor
    *  - terminate itself when all workers finish counting
    */
  class AprioriActor(items: IndexedSeq[Item], terminationChecker: Promise[Boolean]) extends Actor {

    private var workers = Router(RoundRobinRoutingLogic())

    private var itemsets = mutable.Queue.empty[ItemSet]
    private var currentItem: Int = 0

    private var askedRouters: Int = 0
    private var stoppedRouters: Int = 0

    def receive: Receive = {
      //Register a new worker to the worker collection
      case RegisterWorker(actorRef) =>
        if (actorRef.compareTo(self) != 0) workers = workers.addRoutee(actorRef)
      //Some worker has no work
      //If we have enough works we divide our work and send the part back to the sender
      //If we have little work we send the NoWork message
      case AskForWork =>
        val splittingPoint = itemsets.size / 2
        if (splittingPoint > 0) {
          val (part1, part2) = itemsets.splitAt(splittingPoint)
          itemsets = part1
          if (part2.isEmpty) sender() ! NoWork else sender() ! Work(part2)
        } else {
          sender() ! NoWork
        }
      //We obtained work which we add to the queue
      //Then we send an empty message to itself for starting and processing work
      case Work(data) =>
        itemsets.enqueue(data: _*)
        askedRouters = 0
        self ! ""
      //We have no work and we obtained NoWork message from some asking actor
      //Then we try to ask another actor
      //If we asked all actors we send Stop message to all actors
      case NoWork =>
        askedRouters += 1
        if (askedRouters >= workers.routees.size) {
          workers.route(Broadcast(Stop), self)
          self ! Stop
        } else {
          workers.route(AskForWork, self)
        }
      //We obtained Stop message which indicates that some actor has completed all jobs and is idle
      //Once we obtained Stop messages from all workers we can terminate this actor and fill the termination checker Future object
      case Stop =>
        stoppedRouters += 1
        if (stoppedRouters >= workers.routees.size + 1) {
          context stop self
          terminationChecker.success(true)
        }
      //Any other message means that we should expand the current itemset in the queue with the current item
      case _ => itemsets.headOption match {
        //There is an itemset in the queue
        case Some(itemSet) => items.lift(currentItem) match {
          //There is an unprocessed item for the current itemset
          //We expand the current itemset with the current item
          //If the new itemset satisfies the min support threshold we add it to the queue
          //Finaly we move to the next item and process it
          case Some(item) =>
            itemSet.expandWith(item).filter(_.relativeSupport >= minSupport).foreach { newItemSet =>
              itemsets.enqueue(newItemSet)
            }
            currentItem += 1
            self ! ""
          //No available item for the current itemset
          //We dequeue the current itemset and process the other one
          case None =>
            currentItem = 0
            println(itemsets.dequeue())
            counter.incrementAndGet()
            self ! ""
        }
        //No itemset in the queue we should ask some actor for other work
        case None => workers.route(AskForWork, self)
      }
    }
  }

  object AprioriActor {

    def props(items: IndexedSeq[Item], terminationChecker: Promise[Boolean]): Props = Props(new AprioriActor(items, terminationChecker))

    /**
      * Abstraction for all messages which the AprioriActor is able to process
      */
    sealed trait Message

    case class RegisterWorker(actorRef: ActorRef) extends Message

    case class Work(itemsets: Seq[ItemSet]) extends Message

    object AskForWork extends Message

    object NoWork extends Message

    object Stop extends Message

  }

  def main(args: Array[String]): Unit = {
    /**
      * Start count mining time
      */
    val startTime = System.currentTimeMillis()

    /**
      * Load all transactions from a dataset
      */
    implicit val transactions: Transactions = Transactions.fromCsv(new File("KO_Bank_all.csv"))

    /**
      * Count the number of availaible cores
      */
    val cores = Runtime.getRuntime.availableProcessors()

    /**
      * Get all items from transactions which have relative support greater or equal than min support
      * This operation is processed in parallel!
      * For simplification we use the parallel collection
      */
    val itemsSets = transactions.items.par.flatMap(ItemSet().expandWith).filter(_.relativeSupport >= minSupport).toIndexedSeq
    val items = itemsSets.flatMap(_.items)

    /**
      * Start an actor system
      */
    val actorSystem = ActorSystem("my-actor-system")
    implicit val ec: ExecutionContext = actorSystem.dispatcher

    /**
      * Class for worker
      * Worker is an actor which is able to enumerate obtained itemsets
      * This class creates a worker and wraps it into an object which contains:
      *  - reference to the worker actor,
      *  - Future object which indicates termination of the worker,
      *  - and piece of work (sequence of itemsets) which the worker starts with
      */
    class Worker(val work: Seq[ItemSet]) {
      val (terminationChecker, actor) = {
        val terminationChecker = Promise[Boolean]()
        terminationChecker.future -> actorSystem.actorOf(AprioriActor.props(items, terminationChecker))
      }
    }

    /**
      * First we split all items to "p" groups where "p" is the number of cores
      * For each this group we create a worker which own the particular piece of work
      */
    val workers = itemsSets.toGroups(cores).map(new Worker(_)).toList

    /**
      * We send actor references of all workers to all workers
      */
    workers.foreach(w1 => workers.foreach(w2 => w1.actor ! RegisterWorker(w2.actor)))
    /**
      * Then we send particular works to all workers
      * This action starts the itemset enumeration process which is doing in parallel
      * Each actor has its own work and represents just one thread
      * Once any actor completes its work then it asks other actors for other work
      */
    workers.foreach(w => w.actor ! Work(w.work))

    /**
      * When all actors are completed they fill their terminationChecker Future object
      * As soon as all termination checkers are completed we stop the actor context
      */
    val terminationChecker = Future.sequence(workers.map(_.terminationChecker))
    Await.result(terminationChecker, Duration.Inf)

    println(s"Number of rules: ${counter.get()}")
    println(s"Mining time: ${(System.currentTimeMillis() - startTime) / 1000}s")

    actorSystem.terminate()
  }

}
