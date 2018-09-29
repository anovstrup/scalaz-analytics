package scalaz.analytics.example

import scalaz.analytics.local._
import scalaz.zio.App
import scalaz.zio.console._

/**
  * This is a temporary example to show a basic example of a usage.
  */
object SimpleExample extends App {

  override def run(args: List[String]) =
    doit().attempt.map(_.fold(e => {e.printStackTrace(); 1}, _ => 0)).map(ExitStatus.ExitNow(_))

  def doit() = {
    println(ds1)

    for {
      seq <- execute(ds1)
      _ <- putStrLn(seq.mkString(","))
      fold <- execute(folded)
      _ <- putStrLn(fold.mkString(","))
    } yield ()
  }

  val ds1: DataSet[Int] =
    fromIterable(Seq(4, 3, 2, 1))
      .map(i => ((i * 2) + 1) - 1)
      .filter(i => i > 2)
      .distinct

  val folded: DataSet[Int] =
    ds1
      .fold(0) { case (i, acc) => acc + i }

  val ds2: DataStream[Int] =
    emptyStream[Int]
      .filter(i => i + 1 > 0)
      .distinct(Window.FixedTimeWindow())
}
