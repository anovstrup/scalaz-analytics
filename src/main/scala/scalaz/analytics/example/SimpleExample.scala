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

  def doit() =
    for {
      _ <- putStrLn(ds1.toString)
      seq <- execute(ds1)
      _ <- putStrLn(seq.mkString(","))
      _ <- putStrLn(folded.toString)
      fold <- execute(folded)
      _ <- putStrLn("dsBad: " + dsBad.toString)
      badFold <- execute(dsBad)
      _ <- putStrLn(badFold mkString ",")
      _ <- putStrLn(fold.mkString(","))
      _ <- putStrLn(ds2.toString)
      _ <- putStrLn(tupleExample.toString)
      _ <- putStrLn(tupleExample2.toString)
    } yield ()

  val ds1: DataSet[Int] =
    fromIterable(Seq(4, 3, 2, 1))
      .map(i => ((i * 2) + 1) - 1)
      .filter(i => i > 2)
      .distinct

  val folded: DataSet[Int] =
    ds1
      .fold(0) { xy => xy._1 + xy._2 }

  val dsBad: DataSet[String] =
    fromIterable(Seq("a", "b", "c", "d")).fold("")(ba => ba._2 concat ba._1)

  val ds2: DataStream[Int] =
    emptyStream[Int]
      .filter(i => i + 1 > 0)
      .distinct(Window.FixedTimeWindow())

  val tupleExample: DataSet[(Int, Boolean)] =
    empty[(Int, String)]
      .map(_ => (4, false))
      .filter(_._2)

  val tupleExample2: DataSet[(Int, String)] =
    empty[(Int, String)]
      .map(_ => (4, false)) // tuple of literals works
      .map(s => (3, s._1)) // tuple with right side projection
      .map(s => (s._2, "")) // tuple with left side projection

  val ds3: DataStream[Int] =
    emptyStream[Unknown]
    .map(_ => column[Int]("apples") + column[Int]("bananas"))
}
