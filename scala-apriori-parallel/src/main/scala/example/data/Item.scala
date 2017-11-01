package example.data

/**
  * Created by Vaclav Zeman on 31. 10. 2017.
  */
case class Item(attribute: String, value: String) {
  override def toString: String = attribute + "=" + value
}

object Item {

  implicit def itemOrdering(implicit transactions: Transactions): Ordering[Item] = {
    val map = transactions.items.iterator.zipWithIndex.toMap
    Ordering.by[Item, Int](map.apply)
  }

}
