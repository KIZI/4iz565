package example.data

/**
  * Created by Vaclav Zeman on 6. 11. 2017.
  */
case class OrderedItemSet(headItem: Item, items: Set[Item], support: Int, relativeSupport: Double) extends ItemSet {

  def +(item: Item)(implicit o: Ordering[Item]): Option[OrderedItemSet] = if (o.gt(item, headItem)) {
    Some(copy(headItem = item, items = items + item))
  } else {
    None
  }

  def withSupport(support: Int)(implicit di: DatabaseInfo): OrderedItemSet = copy(support = support, relativeSupport = di.relativeSupport(support))

}

object OrderedItemSet {

  def apply(support: Int, item: Item)(implicit di: DatabaseInfo): OrderedItemSet = new OrderedItemSet(item, Set(item), support, di.relativeSupport(support))

}
