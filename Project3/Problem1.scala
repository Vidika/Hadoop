package comp9313.ass3
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//Start of the question 
object Problem1{
  def main(args:Array[String]){
    val inputFile=args(0)//Input define in argument
    val outputFolder=args(1)//Output folder defined in argument
    val conf=new //New configuration
SparkConf().setAppName("Problem1").setMaster("local")//Set App name
  val sc=new SparkContext(conf) //New Spark Context
  val graph =sc.textFile(inputFile).map(_.split(" "))//Split the graph file
  val node=1//Node value
  val distance=3//Distance value
  val node_distance=graph.map(x=>(x(node).toInt,(x(distance).toDouble,1)))//for every node make a tuple with distance and 1
  val average_incoming=node_distance.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(x=>x._1/x._2)//Reduce and map the averages of all distances of all outgoing nodes for particular node
  val sorted_average=average_incoming.sortBy(_._1).sortBy(_._2,false)//Sorting on descending order on distance and ascending order on key values
  val format_output=sorted_average //Format the output in the way that is required as per spicification
  .map(x => x._1+"\t"+x._2)
  .saveAsTextFile(outputFolder)//Save to output folder
}
}//End of the object
