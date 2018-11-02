package scalaz.analytics.example

import scalaz.analytics.local._
import scalaz.zio.{App, IO}
import scalaz.zio.console._

import scala.io.Source

object WordCount extends App {
  def run(args: List[String]) =
    doit(args.headOption).attempt.map(_.fold(e => {e.printStackTrace(); 1}, _ => 0)).map(ExitStatus.ExitNow)

  def doit(inputFileOpt: Option[String]): IO[Throwable, Unit] = for {
    inputFile <- IO.fromOption(inputFileOpt).leftMap(_ => new RuntimeException("No input file specified"))
    linesDs <- lines(inputFile)
    ds = wordCount(fromIterable(linesDs))
    _ <- putStrLn(ds.toString)
    res <- execute(ds)
    _ <- putStrLn(res mkString "\n")
  } yield ()

  def wordCount(linesDataset: DataSet[String]): DataSet[(String, Int)] =
    linesDataset
      .flatMap[String](_.split(" "))
      .aggregate(0)(_._1 + 1)

  def lines(inputFile: String): IO[Throwable, Seq[String]] = for {
    ls <- IO.now(Seq("this is a", "test of the", "emergency broadcast system", "this is", "only a test"))
    _ <- putStrLn(inputFile)
  } yield ls
//    readLines(inputFile)

  def readLines(filename: String): IO[Throwable, Seq[String]] =
    IO.syncThrowable { Source.fromFile(filename).getLines.toSeq }
}
