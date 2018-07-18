package comp9313.ass3
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._  //Import the graphx library
//Reference used:https://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api
//Reference used:https://www.cakesolutions.net/teamblogs/graphx-pregel-api-an-example
object Problem2{
  def main(args:Array[String]){
    val inputFile=args(0)//Input specified in arguments
    val conf=new //New configuration
SparkConf().setAppName("Problem2").setMaster("local")//Setting the app name
  val sc=new SparkContext(conf)
    val fileName = args(0)//File name is argument  0
    val k = args(1).toInt//The source input in arguments
    val edges = sc.textFile(fileName)//Value edges
    val edgelist = edges.map(x => x.split(" ")).map(x=> Edge(x(1).toLong, x(2).toLong,1))	//Make Edge List
    val graph = Graph.fromEdges[Int, Int](edgelist,1)//Make a graph with Int Int
	  val initialGraph = graph.mapVertices((id, _) => if(id==k) 1.0 else 0.0)//If its the source node then 1 else 0
    val result = initialGraph.pregel(0.0)((id, cv, nv) => math.max(cv,nv), triplet => {if (triplet.srcAttr==1 && triplet.dstAttr==0){Iterator((triplet.dstId, triplet.srcAttr))}else{Iterator.empty}}, (a,b) => math.max(a,b))
    //The message sent, message received and merging of messages is defined in the step above
    //If the source attribute is 1 and target attribute is zero that's when we send message 
    val values=result.vertices.filter{case(id,value)=>value==1.0}//Filtering the result based on connected nodes which have value 1       
    val final_count=values.count()-1//Final count is one less than the actual count as it includes the actual node as well
    println(final_count)//Print the final count
    
    
  }
}//End of the object
    
 


