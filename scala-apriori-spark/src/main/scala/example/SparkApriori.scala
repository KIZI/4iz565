package example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vaclav Zeman on 2. 11. 2017.
  */
object SparkApriori {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("apriori").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val result = sc.parallelize(0 to 10).sum()

    println(result.toLong)

  }

}
