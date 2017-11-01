package example.data

/**
  * Created by Vaclav Zeman on 31. 10. 2017.
  */
class ItemSet private(_items: List[Item])(implicit tx: Transactions, o: Ordering[Item]) {

  lazy val items: Set[Item] = _items.toSet

  lazy val support: Int = tx.data.count(x => items.subsetOf(x.items))

  lazy val relativeSupport: Double = support.toDouble / tx.length

  def expandWith(item: Item): Option[ItemSet] = _items.headOption.orElse(Some(item))
    .filter(x => _items.isEmpty || (o.gt(item, x) && !_items.iterator.map(_.attribute).contains(item.attribute)))
    .map(_ => new ItemSet(item :: _items))

  override def toString: String = "{" + _items.reverseIterator.mkString(", ") + "}, support: " + relativeSupport

}

object ItemSet {

  def apply()(implicit transactions: Transactions): ItemSet = new ItemSet(Nil)

}
