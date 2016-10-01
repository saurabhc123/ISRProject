import org.scalatest.FunSuite

class SparkGrepTest extends FunSuite {

  test("Run the test") {
    SparkGrep.main(Array("local[*]", "src/main/java/scala/SparkGrep.scala", "val"))
  }

}
