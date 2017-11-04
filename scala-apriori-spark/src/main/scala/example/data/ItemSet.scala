package example.data

/**
  * Created by Vaclav Zeman on 31. 10. 2017.
  */
case class ItemSet(items: Set[Item], support: Int, relativeSupport: Double) {
  override def toString = s"{${items.mkString(", ")}}, support: $support, relative support: $relativeSupport"
}

object ItemSet {

  def apply(support: Int, items: Set[Item])(implicit di: DatabaseInfo): ItemSet = ItemSet(items, support, di.relativeSupport(support))

  def apply(support: Int, items: Item*)(implicit di: DatabaseInfo): ItemSet = apply(support, items.toSet)

}
