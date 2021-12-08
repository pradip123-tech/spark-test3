import scala.io.StdIn._
object Prac extends App {

  val input=readLine()

  // reverse the entire line
  val output1=input.reverse

  println(output1)
  //reverse the each word

  val output2=input.split(" ").map(_.reverse)

  println(output2)
}
