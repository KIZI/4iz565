package example.utils

import scala.collection.IterableLike

/**
  * Created by Vaclav Zeman on 1. 11. 2017.
  */
object CollectionExtensions {

  implicit class PimpedCollection[T, Repr](coll: IterableLike[T, Repr]) {
    def toGroups(n: Int): Iterator[Repr] = coll.grouped(math.ceil(coll.size.toDouble / n).toInt)
  }

}
