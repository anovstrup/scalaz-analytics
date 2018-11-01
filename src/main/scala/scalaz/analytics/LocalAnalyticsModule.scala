package scalaz.analytics

import scalaz.zio.IO
import scala.language.implicitConversions
import java.time.{Instant, LocalDate}
import scala.math.Numeric._

/**
 * A non distributed implementation of Analytics Module
 */
trait LocalAnalyticsModule extends AnalyticsModule {
  override type DataSet[A]    = LocalDataStream
  override type DataStream[A] = LocalDataStream
  override type Type[A]       = LocalType[A]
  override type Unknown       = UnknownType
  override type =>:[-A, +B]   = RowFunction
  type UnknownType

  private object LocalNumeric {

    def apply[A: Type]: Numeric[A] =
      new Numeric[A] {
        override val typeOf: Type[A]                 = Type[A]
        override def mult: (A, A) =>: A              = RowFunction.Mult(Type[A].reified)
        override def sum: (A, A) =>: A               = RowFunction.Sum(Type[A].reified)
        override def diff: (A, A) =>: A              = RowFunction.Diff(Type[A].reified)
        override def mod: (A, A) =>: A               = RowFunction.Mod(Type[A].reified)
        override def greaterThan: (A, A) =>: Boolean = RowFunction.GreaterThan(Type[A].reified)
      }
  }

  implicit override val unknown: Type[Unknown] = new Type[Unknown] {
    override def reified: Reified = Reified.Unknown
  }
  implicit override val intType: Type[scala.Int]       = LocalType(Reified.Int)
  implicit override val intNumeric: Numeric[scala.Int] = LocalNumeric[Int]

  implicit override val longType: Type[scala.Long]       = LocalType(Reified.Long)
  implicit override val longNumeric: Numeric[scala.Long] = LocalNumeric[scala.Long]

  implicit override val floatType: Type[scala.Float]       = LocalType(Reified.Float)
  implicit override val floatNumeric: Numeric[scala.Float] = LocalNumeric[scala.Float]

  implicit override val doubleType: Type[scala.Double]       = LocalType(Reified.Double)
  implicit override val doubleNumeric: Numeric[scala.Double] = LocalNumeric[scala.Double]

  implicit override val decimalType: LocalType[scala.math.BigDecimal] = LocalType(
    Reified.BigDecimal
  )
  implicit override val decimalNumeric: Numeric[scala.math.BigDecimal] =
    LocalNumeric[scala.BigDecimal]

  implicit override val stringType: Type[scala.Predef.String] = LocalType(Reified.String)
  implicit override val booleanType: Type[scala.Boolean]      = LocalType(Reified.Boolean)
  implicit override val byteType: Type[scala.Byte]            = LocalType(Reified.Byte)
  implicit override val nullType: Type[scala.Null]            = LocalType(Reified.Null)
  implicit override val shortType: Type[scala.Short]          = LocalType(Reified.Short)
  implicit override val instantType: Type[Instant]            = LocalType(Reified.Instant)
  implicit override val dateType: Type[LocalDate]             = LocalType(Reified.LocalDate)

  implicit override def tuple2Type[A: Type, B: Type]: Type[(A, B)] = new Type[(A, B)] {
    override def reified: Reified = Reified.Tuple2(LocalType.typeOf[A], LocalType.typeOf[B])
  }

  /**
   * A typeclass that produces a Reified
   */
  sealed trait LocalType[A] {
    def reified: Reified
  }

  object LocalType {
    def typeOf[A](implicit ev: LocalType[A]): Reified = ev.reified

    private[LocalAnalyticsModule] def apply[A](r: Reified): Type[A] =
      new Type[A] {
        override def reified: Reified = r
      }
  }

  /**
   * The set of reified types.
   * These represent all the Types that scalaz-analytics works with
   */
  sealed trait Reified

  object Reified {
    case object Int                           extends Reified
    case object Long                          extends Reified
    case object Float                         extends Reified
    case object Double                        extends Reified
    case object BigDecimal                    extends Reified
    case object String                        extends Reified
    case object Decimal                       extends Reified
    case object Boolean                       extends Reified
    case object Byte                          extends Reified
    case object Null                          extends Reified
    case object Short                         extends Reified
    case object Instant                       extends Reified
    case object LocalDate                     extends Reified
    case object Unknown                       extends Reified
    case class Tuple2(a: Reified, b: Reified) extends Reified
  }

  /**
   * A reified DataStream program
   */
  sealed trait LocalDataStream

  object LocalDataStream {
    case class Empty(rType: Reified) extends LocalDataStream

    case class Iter[A](rType: Reified, data: Iterable[A]) extends LocalDataStream

    case class Map(d: LocalDataStream, f: RowFunction)    extends LocalDataStream
    case class Filter(d: LocalDataStream, f: RowFunction) extends LocalDataStream

    case class Fold(d: LocalDataStream, initial: RowFunction, f: RowFunction, window: Window)
        extends LocalDataStream
    case class Distinct(d: LocalDataStream, window: Window) extends LocalDataStream
  }

  private val ops: Ops[DataSet] = new Ops[DataSet] {
    override def map[A, B](ds: LocalDataStream)(f: A =>: B): LocalDataStream =
      LocalDataStream.Map(ds, f)
    override def filter[A](ds: LocalDataStream)(f: A =>: Boolean): LocalDataStream =
      LocalDataStream.Filter(ds, f)

    override def fold[A, B](ds: LocalDataStream)(window: Window)(initial: B =>: B)(
      f: A =>: B
    ): LocalDataStream = LocalDataStream.Fold(ds, initial, f, window)
    override def distinct[A](ds: LocalDataStream)(window: Window): LocalDataStream =
      LocalDataStream.Distinct(ds, window)
  }

  override val setOps: Ops[DataSet]       = ops
  override val streamOps: Ops[DataStream] = ops

  /**
   * An implementation of the arrow (=>:) in AnalyticsModule
   * This allows us to reify all the operations.
   */
  sealed trait RowFunction

  object RowFunction {
    case class Id(reifiedType: Reified)                       extends RowFunction
    case class Compose(left: RowFunction, right: RowFunction) extends RowFunction
    case class Mult(typ: Reified)                             extends RowFunction
    case class Sum(typ: Reified)                              extends RowFunction
    case class Diff(typ: Reified)                             extends RowFunction
    case class Mod(typ: Reified)                              extends RowFunction
    case class GreaterThan(typ: Reified)                      extends RowFunction
    case class FanOut(fst: RowFunction, snd: RowFunction)     extends RowFunction
    case class Split(f: RowFunction, g: RowFunction)          extends RowFunction
    case class Product(fab: RowFunction)                      extends RowFunction
    case class Column(colName: String, rType: Reified)        extends RowFunction
    case class ExtractFst(reified: Reified)                   extends RowFunction
    case class ExtractSnd(reified: Reified)                   extends RowFunction

    // constants
    case class IntLiteral(value: Int)                            extends RowFunction
    case class LongLiteral(value: Long)                          extends RowFunction
    case class FloatLiteral(value: Float)                        extends RowFunction
    case class DoubleLiteral(value: Double)                      extends RowFunction
    case class DecimalLiteral(value: BigDecimal)                 extends RowFunction
    case class StringLiteral(value: String)                      extends RowFunction
    case class BooleanLiteral(value: Boolean)                    extends RowFunction
    case class ByteLiteral(value: Byte)                          extends RowFunction
    case object NullLiteral                                      extends RowFunction
    case class ShortLiteral(value: Short)                        extends RowFunction
    case class InstantLiteral(value: Instant)                    extends RowFunction
    case class LocalDateLiteral(value: LocalDate)                extends RowFunction
    case class Tuple2Literal(fst: RowFunction, snd: RowFunction) extends RowFunction
  }

  override val stdLib: StandardLibrary = new StandardLibrary {
    override def id[A: Type]: A =>: A = RowFunction.Id(LocalType.typeOf[A])

    override def compose[A, B, C](f: B =>: C, g: A =>: B): A =>: C = RowFunction.Compose(f, g)

    override def andThen[A, B, C](f: A =>: B, g: B =>: C): A =>: C = RowFunction.Compose(g, f)

    override def fanOut[A, B, C](fst: A =>: B, snd: A =>: C): A =>: (B, C) =
      RowFunction.FanOut(fst, snd)

    override def split[A, B, C, D](f: A =>: B, g: C =>: D): (A, C) =>: (B, D) =
      RowFunction.Split(f, g)

    override def product[A, B](fab: A =>: B): (A, A) =>: (B, B) = RowFunction.Product(fab)

    override def fst[A: Type, B]: (A, B) =>: A = RowFunction.ExtractFst(Type[A].reified)

    override def snd[A, B: Type]: (A, B) =>: B = RowFunction.ExtractSnd(Type[B].reified)
  }

  override def empty[A: Type]: LocalDataStream = LocalDataStream.Empty(LocalType.typeOf[A])

  override def emptyStream[A: Type]: LocalDataStream = LocalDataStream.Empty(LocalType.typeOf[A])

  override def fromIterable[A: LocalType](iterable: Iterable[A]): LocalDataStream = LocalDataStream.Iter[A](LocalType.typeOf[A], iterable)

  implicit override def int[A](v: scala.Int): A =>: Int          = RowFunction.IntLiteral(v)
  implicit override def long[A](v: scala.Long): A =>: Long       = RowFunction.LongLiteral(v)
  implicit override def float[A](v: scala.Float): A =>: Float    = RowFunction.FloatLiteral(v)
  implicit override def double[A](v: scala.Double): A =>: Double = RowFunction.DoubleLiteral(v)
  implicit override def decimal[A](v: scala.BigDecimal): A =>: BigDecimal =
    RowFunction.DecimalLiteral(v)
  implicit override def string[A](v: scala.Predef.String): A =>: String =
    RowFunction.StringLiteral(v)
  implicit override def boolean[A](v: scala.Boolean): A =>: Boolean = RowFunction.BooleanLiteral(v)
  implicit override def byte[A](v: scala.Byte): A =>: Byte          = RowFunction.ByteLiteral(v)
  implicit override def `null`[A](v: scala.Null): A =>: Null        = RowFunction.NullLiteral
  implicit override def short[A](v: scala.Short): A =>: Short       = RowFunction.ShortLiteral(v)
  implicit override def instant[A](v: Instant): A =>: Instant       = RowFunction.InstantLiteral(v)
  implicit override def localDate[A](v: LocalDate): A =>: LocalDate = RowFunction.LocalDateLiteral(v)
  implicit override def tuple2[A, B, C](t: (A =>: B, A =>: C)): A =>: (B, C) =
    RowFunction.Tuple2Literal(t._1, t._2)

  // todo this needs more thought
  override def column[A: Type](str: String): Unknown =>: A =
    RowFunction.Column(str, LocalType.typeOf[A])

  def load(path: String): DataStream[Unknown] = ???

  def execute[A](dataStream: DataSet[A]): IO[Throwable, Seq[A]] =
    localRun(dataStream)

  def localRun[A](dataStream: LocalDataStream): IO[Throwable, Seq[A]] =
    compile(dataStream)

  def compile[A](dataStream: LocalDataStream): IO[Throwable, Seq[A]] = dataStream match {
    case LocalDataStream.Empty(_) =>
      IO.point(Seq.empty[A])
    case LocalDataStream.Iter(_, data) =>
      IO.point(data.asInstanceOf[Iterable[A]].toSeq)
    case LocalDataStream.Map(data, fun) =>
      val funCompiled = compile[A, A](fun)
      compile(data).flatMap(seq => funCompiled.map(seq.map(_)))
    case LocalDataStream.Filter(data, fun) =>
      val funCompiled = compile[A, Boolean](fun)
      compile(data).flatMap(seq => funCompiled.map(seq.filter))
    case LocalDataStream.Distinct(data, _) =>
      compile(data).map(_.distinct)
    case LocalDataStream.Fold(data, init, fun, _) =>
      val initCompiled = compile[A, A](init)
      val funCompiled = compile2[A, A](fun)
      compile(data).flatMap(seq => initCompiled.par(funCompiled).map {
        case (zero, f) => Seq(seq.fold(zero(null.asInstanceOf[A]))(f))
      })
    case _ =>
      IO.fail(new IllegalArgumentException("something goes wrong"))
  }

  def compile[A, B](rowFunction: RowFunction): IO[Throwable, A => B] = rowFunction match {
    case lit if isLiteral(lit) =>
      literal[B](lit).flatMap(mkEmptyFun1)
    case RowFunction.Id(_) =>
      IO.point(identityFun[A, B])
    case RowFunction.Compose(op, RowFunction.FanOut(left, right)) if isBinaryOp(op) =>
      compileOp[A, B](op).par(compile[A, A](left)).par(compile[A, A](right)).map {
        case ((fun, l), r) => x: A => fun(l(x), r(x))
      }
  }

  def compile2[A, B](rowFunction: RowFunction): IO[Throwable, (A, A) => B] = rowFunction match {
    case RowFunction.Compose(op, RowFunction.FanOut(left, right)) if isBinaryOp(op) =>
      compileOp[A, B](op).par(compile[A, A](left)).par(compile[A, A](right)).map {
        case ((fun, l), r) => (x: A, y: A) => fun(l(x), r(y))
      }
  }

  def compileOp[A, B](operator: RowFunction): IO[Throwable, (A, A) => B] = operator match {
    case RowFunction.Mult(rType) => integral[A](rType).map(mulFun)
    case RowFunction.Sum(rType) => integral[A](rType).map(sumFun)
    case RowFunction.Diff(rType) => integral[A](rType).map(diffFun)
    case RowFunction.Mod(rType) => integral[A](rType).map(modFun)
    case RowFunction.GreaterThan(rType) => integral[A](rType).map(greaterThan)
    case _ => IO.fail(new NotImplementedError)
  }

  def literal[A](rowFunction: RowFunction): IO[Throwable, A] = rowFunction match {
    case RowFunction.ByteLiteral(value) => IO.now(value.asInstanceOf[A])
    case RowFunction.ShortLiteral(value) => IO.now(value.asInstanceOf[A])
    case RowFunction.IntLiteral(value) => IO.now(value.asInstanceOf[A])
    case RowFunction.LongLiteral(value) => IO.now(value.asInstanceOf[A])
    case RowFunction.FloatLiteral(value) => IO.now(value.asInstanceOf[A])
    case RowFunction.DoubleLiteral(value) => IO.now(value.asInstanceOf[A])
    case RowFunction.DecimalLiteral(value) => IO.now(value.asInstanceOf[A])
    case RowFunction.BooleanLiteral(value) => IO.now(value.asInstanceOf[A])
    case RowFunction.StringLiteral(value) => IO.now(value.asInstanceOf[A])
    case RowFunction.NullLiteral => IO.now(null.asInstanceOf[A])
    case _ => IO.fail(new IllegalStateException(s"$rowFunction is not literal"))
  }

  def isLiteral(rowFunction: RowFunction): Boolean = rowFunction match {
    case RowFunction.IntLiteral(_) => true
    case RowFunction.LongLiteral(_) => true
    case RowFunction.FloatLiteral(_) => true
    case RowFunction.DoubleLiteral(_) => true
    case RowFunction.DecimalLiteral(_) => true
    case RowFunction.StringLiteral(_) => true
    case RowFunction.BooleanLiteral(_) => true
    case RowFunction.ByteLiteral(_) => true
    case RowFunction.NullLiteral => true
    case RowFunction.ShortLiteral  (_) => true
    case _ => false
  }

  def isBinaryOp(rowFunction: RowFunction): Boolean = rowFunction match {
    case RowFunction.Mult(_) => true
    case RowFunction.Sum(_) => true
    case RowFunction.Diff(_) => true
    case RowFunction.Mod(_) => true
    case RowFunction.GreaterThan(_) => true
    case _ => false
  }

  def integral[A](reified: Reified): IO[Throwable, Integral[A]] =
    reified match {
      case Reified.Byte => IO.now(ByteIsIntegral.asInstanceOf[scala.Integral[A]])
      case Reified.Short => IO.now(ShortIsIntegral.asInstanceOf[scala.Integral[A]])
      case Reified.Int => IO.now(IntIsIntegral.asInstanceOf[scala.Integral[A]])
      case Reified.Long => IO.now(LongIsIntegral.asInstanceOf[scala.Integral[A]])
      case Reified.Float => IO.now(FloatAsIfIntegral.asInstanceOf[scala.Integral[A]])
      case Reified.Double => IO.now(DoubleAsIfIntegral.asInstanceOf[scala.Integral[A]])
      case Reified.BigDecimal => IO.now(BigDecimalAsIfIntegral.asInstanceOf[scala.Integral[A]])
      case _ => IO.fail(new IllegalStateException(s"$reified doesn't have integral"))
    }

  def mulFun[A, B](integral: Integral[A]): (A, A) => B = (x: A, y: A) =>
    integral.times(x, y).asInstanceOf[B]

  def sumFun[A, B](integral: Integral[A]): (A, A) => B = (x: A, y: A) =>
    integral.plus(x, y).asInstanceOf[B]

  def diffFun[A, B](integral: Integral[A]): (A, A) => B = (x: A, y: A) =>
    integral.minus(x, y).asInstanceOf[B]

  def modFun[A, B](integral: Integral[A]): (A, A) => B = (x: A, y: A) =>
    integral.rem(x, y).asInstanceOf[B]

  def greaterThan[A, B](integral: Integral[A]): (A, A) => B = (x: A, y: A) =>
    integral.gt(x, y).asInstanceOf[B]

  def identityFun[A, B](a: A): B = a.asInstanceOf[B]

  def mkEmptyFun1[A, B](b: B): IO[Throwable, A => B] = IO.point {
    _: A => b
  }
}
