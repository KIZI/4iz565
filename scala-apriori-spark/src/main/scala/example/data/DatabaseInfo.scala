package example.data

/**
  * Created by Vaclav Zeman on 3. 11. 2017.
  */
case class DatabaseInfo(databaseSize: Int) {
  def relativeSupport(support: Int): Double = support.toDouble / databaseSize
}
