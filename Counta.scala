import scala.collection.mutable.ListBuffer
import scala.io.StdIn.{readInt, readLine}

object Counta extends App {

  println("Enter numbers continuously by pressing Return carriage")
  var collectedNums = new ListBuffer[Int]()
  var grab = readLine()
  collectedNums += grab.toInt

  while (true) {
    grab = readLine()
    collectedNums += grab.toInt
    println(collectedNums)
    println(collectedNums.distinct.sortWith(_ > _).take(4))

  }

  println(collectedNums)
}
