package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap



object SetSimJoin {
  def main(args:Array[String]){
   val inputFile=args(0)//Input file argument
   val outputFolder=args(1)//Output Folder
   val conf=new
SparkConf().setAppName("SimSimilarity").setMaster("local")
   val sc=new SparkContext(conf)
   val textfile =sc.textFile(inputFile)
   val lines=textfile.map(x=>x.split(" "))//Split Input file
   val threshold=args(2).toDouble//threshold argument
   //Procedure for frequency sort starts here
   val counts=lines.flatMap(x=>x.tail)//Create a flatmap values
   val count_number=counts.map(x=>(x.toInt,1))//to count map everything with 1
   val frequency=count_number.countByKey()//Count by key values
   val to_list=frequency.toList//COnvert the answer to list
   val sorted_array=to_list.sortBy(_._2).map({case(a,b)=>a})//create a sorted array based on frequency
   val c=sorted_array.zipWithIndex//Zip the values with their index
   val hm=c.toMap//convert it to map
   //val change_to_list=sorted_array.collect.toList
   //*val broadcast_list=sc.broadcast(sorted_array)
   val row_id=lines.map(x=>(x(0).toInt,x.drop(1).map(_.toInt).sortBy(a=>hm(a))))//Sort the values of array based on global order
   val map_row_id=row_id.map(x=>(x._1,x._2)).collectAsMap//Collect them as Map
   //val broadcast_variable=sc.broadcast(map_row_id)
   val row_threshold=lines.map(x=>(x(0).toInt,(x.length-1)-Math.ceil((x.length-1)*threshold).toInt+1))//Calculate the prefix length and store it along with row id as key and prefix length as value
   val jointwo=row_id.join(row_threshold).map({case(a,(b,c))=>(a,b.take(c))})//In the initial row id and array map just take the array upto the prefix length from start
   //val row_id=lines.map(x=>(x(0).toInt,x.drop(1).map(_.toInt).sortBy(a=>hm(a))))
   val id_token=row_id.flatMapValues(x=>x)//Flat map row id with each array token
   val token_id=id_token.map({case(a,b)=>(b,a)})//reverse the keys to value 
    val all_indices=token_id.groupByKey().mapValues(_.toList)//Group them by to find the occurrence of number in respective row id's
    val token_prefix=jointwo.flatMapValues(x=>x)//Repeat on prefixes
    val prefix_token=token_prefix.map({case(a,b)=>(b,a)})
  val prefix_indices=prefix_token.groupByKey().mapValues(_.toList)
  //Overlap function taken from the paper 
   def overlap_constraint[A](a:List[A],b:List[A],t:Double):Int={Math.ceil(((t/(1.0+t))*(a.size+b.size))).toInt}
  //val combinations_to_match=prefix_indices.map(pair=>(pair._1,pair._2.combinations(2).toList)).collectAsMap
   val combinations_to_match_2=prefix_indices.map(pair=>(pair._1,pair._2.combinations(2).toList))//Find all possible combinations of row-ids
   val alpha_values=combinations_to_match_2.mapValues(a=>a.map(b=>overlap_constraint(map_row_id(b(0)).toList,map_row_id(b(1)).toList,threshold)))//Find overlap constraint for every pair
  //find ubound values on every pair which is (1+min(length_of_the_set(1)*index of the common value on set one,length_of_the_set(2)*index of the common value on set 2)
   val unbound = combinations_to_match_2.map(x=>(x._1,x._2.map(y=>(1 + Math.min(map_row_id(y(0)).length - map_row_id(y(0)).indexOf(x._1.toInt), map_row_id(y(1)).length - map_row_id(y(1)).indexOf(x._1.toInt))))) )
  //Join ubound and alpha values for comparison
  val join_a_u=unbound.join(alpha_values).map{case(a,(b,c))=>(a,b zip c)}
   //If ubound is less than alpha then -1 else the value remains the same
  val comparisons=join_a_u.mapValues(a=>a.map(b=> if(b._1>=b._2){b._2}else{-1}))
  //zip the comparison to respective pair
  val pp=combinations_to_match_2.join(comparisons).map{case(a,(b,c))=>(a,b zip c)}
  val pp_2=pp.mapValues(x=>x.filter(y=>y._2> -1))//Remove if the value is -1
  //Map only candidate pairs
  val candidates = pp_2.mapValues(x=>x.map(y=>y._1))
  val candidate_values = candidates.values
  val pairs_list = candidate_values.map(x=>x.map(y=> y match {case List(a,b)=> (a,b)}))//Change from List() to ()
   val pairs = pairs_list.flatMap(x=>x)//FlatMap the pairs 
  val pairs_final=pairs.distinct()//Fffffinal pairs on which jaccard needs to be applied
  //Jaccard function
  def jaccard[A](a:Set[A],b:Set[A]):Any={var similarity:Double= 0.0; similarity = (a.intersect(b).size.toDouble/a.union(b).size.toDouble); if(similarity>=threshold){return similarity} else{None}}
  val jaccard_on_pairs=pairs_final.map({case(a,b)=>((a,b),jaccard(map_row_id(a).toSet,map_row_id(b).toSet))})
  val final_answer=jaccard_on_pairs.filter{case((a,b),c)=>c!=None}//Filter ones which are not None
  val keys_pairs = final_answer.map(pair => (if (pair._1._1 < pair._1._2) (pair._1) else (pair._1._2, pair._1._1), pair._2))//Swap so if (2,1) make it (1,2)
	val keys_in_order = keys_pairs.sortByKey()//Sort by key
	val ordered_output = keys_in_order.map(pair => pair._1 + "\t" + pair._2)//In the right format
  val format=ordered_output.saveAsTextFile(outputFolder)//Save in output
    
  
  }
}



