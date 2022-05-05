import org.apache.spark.{SparkConf, SparkContext}


object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val conf = new SparkConf().setMaster("yarn").setAppName("hello-spark").setJars(Seq("out/artifacts/hello_spark_jar/hello-spark.jar"))
    val sp =  SparkContext.getOrCreate(conf)
    val strings = {
      sp.makeRDD(Seq(1,2,3,4)).map(_+1).filter(_>3)
    }
    val strings1 = strings.collect()
    strings1.foreach(println)
  }
}