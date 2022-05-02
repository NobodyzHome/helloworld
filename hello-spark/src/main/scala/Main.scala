import org.apache.spark.{SparkConf, SparkContext}


object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val conf = new SparkConf().setMaster("spark://spark-master:7077").setAppName("hello-spark")
    val sp =  SparkContext.getOrCreate(conf)
    val strings = {
      sp.textFile("/Users/maziqiang/Documents/data-center-web-info-part.log", 5).flatMap(_.split(" "))
        .map(Tuple2(_, 1)).reduceByKey(_+_).collect();
    }
    strings.foreach(t=>printf("words=%s,count=%d%n",t._1,t._2))
  }
}