package spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, round}
import org.scalatest.Matchers

trait DataFrameAssertions extends Matchers {

  def assertDataFrameEquals(actual: DataFrame, expected: DataFrame, debug: Boolean = false) = {
    assertDataFrameApproximatelyEquals(actual, expected, Int.MinValue, debug)
  }

  def assertDataFrameApproximatelyEquals(actual: DataFrame, expected: DataFrame, scale: Int, debug: Boolean = false) = {
    val orderedActual = orderApproximatedColAndRow(actual, scale)
    val orderedExpected = orderApproximatedColAndRow(expected, scale)
    val shouldNotBeContained = orderedActual.except(orderedExpected)
    val shouldNotBeContainedError = shouldNotBeContained.count() > 0
    val shouldBeContained = orderedExpected.except(orderedActual)
    val shouldBeContainedError = shouldBeContained.count() > 0

    if(debug){
      println("== actual ==")
      orderedActual.printSchema()
      println("== expected ==")
      orderedExpected.printSchema()
      println("\n\n== actual ==")
      orderedActual.show
      println("== expected ==")
      orderedExpected.show
    }

    val assertionErrorMessage =
      Seq((if (shouldNotBeContainedError) s"following elements are present but should not : ${shouldNotBeContained.collect().map(_.mkString("[", ",", "]")).mkString(" ; ")}" else ""),
        (if (shouldBeContainedError) s"following elements are absent : ${shouldBeContained.collect().map(_.mkString("[", ",", "]")).mkString(" ; ")}" else "")).mkString("\n")

    if (shouldBeContainedError || shouldNotBeContainedError) throw new AssertionError(assertionErrorMessage)
  }

  private def orderApproximatedColAndRow(df: DataFrame, scale: Int): DataFrame = {
    val numericTypes = Set("DoubleType", "LongType", "IntType", "FloatType")
    df.select(df.dtypes.sorted.map{
      case (colName, colType) if scale != Int.MinValue && numericTypes.contains(colType) => round(col(colName), scale)
      case (colName, colType) => col(colName)
    }: _*).orderBy(df.columns.sorted.map(col): _*)
  }

}
