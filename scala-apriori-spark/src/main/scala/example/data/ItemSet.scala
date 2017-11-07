package example.data

/**
  * Created by Vaclav Zeman on 31. 10. 2017.
  */
trait ItemSet {
  val items: Set[Item]
  val support: Int
  val relativeSupport: Double

  override def toString = s"{${items.mkString(", ")}}, support: $support, relative support: $relativeSupport"
}

object ItemSet {

  case class Default(items: Set[Item], support: Int, relativeSupport: Double) extends ItemSet

  def apply(support: Int, items: Set[Item])(implicit di: DatabaseInfo): ItemSet = Default(items, support, di.relativeSupport(support))

  def apply(support: Int, items: Item*)(implicit di: DatabaseInfo): ItemSet = apply(support, items.toSet)

}
