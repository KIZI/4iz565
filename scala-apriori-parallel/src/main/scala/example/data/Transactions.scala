package example.data

import java.io.File

import com.github.tototoshi.csv.CSVReader
import example.impure

/**
  * Created by Vaclav Zeman on 31. 10. 2017.
  */
case class Transactions(data: Seq[Transaction]) {
  val length: Int = data.size
  val items: Set[Item] = data.iterator.flatMap(_.items).toSet
}

object Transactions {

  @impure
  def fromCsv(file: File): Transactions = {
    val reader = CSVReader.open(file)
    try {
      Transactions(
        reader.iteratorWithHeaders
          .map(_.iterator.map(x => Item(x._1, x._2)))
          .map(x => Transaction(x.toSet))
          .toSeq
      )
    } finally {
      reader.close()
    }
  }

}