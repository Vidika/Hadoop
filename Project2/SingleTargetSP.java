package comp9313.ass2;



import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//Counter needed for algorithm.
enum UpdateCounter{
	UpdateDistance
}

public class SingleTargetSP {
	
	
	
	public static String OUT="output";
	public static String IN="input";
	public static double InfDistance=Double.MAX_VALUE;//Initialize the distance as Max(double)
	//Creating adjacency list, in reverse order key is target node and value is the node where it is coming from
	public static class Adjacency_List extends Mapper<Object,Text,Text,Text>{
		private Text node=new Text();
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while(itr.hasMoreTokens()){
			    String[] graph_line=itr.nextToken().split(" ");
			    node.set(graph_line[2]);//Key as Target node
			    context.write(node, new Text(graph_line[1]+":"+graph_line[3]));//coming from node and distance
			    
			}
		}
	}   //Getting the list in the format to do the STSP on the graph.
		public static class Output extends Reducer<Text,Text,Text,Text> {
			public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException{
				String changed_format="";//Change the format to what is needed
				for(Text val:values){
					changed_format +=val+ ",";					
				}
				changed_format=changed_format.substring(0,changed_format.length()-1);
				String visited="0";//Make a label visited to mark the target node
				double distance=InfDistance;//Initialize the distance as infinite(Max distance described)
				int TargetNode=Integer.parseInt(context.getConfiguration().get("TargetNode"));//Get  the target Node
				if(Integer.parseInt(key.toString())==TargetNode){//If one of the key values is Target specified
					visited="1";//Mark it as visited
					distance=0.0;//Distance as zero	
					}
				//Change it to a format that will used for STSP algorithm
				String info="0;"+(new Double(distance)).toString()+";"+visited+";"+(new Integer(TargetNode)).toString()+";"+changed_format;
				context.write(key, new Text(info));//Store for all keys the format.
								
				
			}
		}
		
		//STSP Mapper
		public static class STSPMapper extends Mapper<Object,Text,Text,Text>{
		@Override	
		public void map(Object key,Text value, Context context) throws IOException,InterruptedException{
			String[] info=value.toString().split(";");//Split and input from the previous MapReduce and split it
			context.write(new Text(info[0].split("\t")[0]),value);//Write the values in context
			//System.out.print(context);
			if(Integer.parseInt(info[2])==1){//Check if the node was marked 1 by visited flag
				double distance_from=Double.parseDouble(info[1]);//Get the distance
			if(info.length==5){//if length of the string info is 5
				String[] coming_from=info[4].split(",");//Split it on ','
			for (int i=0;i<coming_from.length;i++){//Explore over length
				String[] in_node_info=coming_from[i].split(":");// split it over : and store in another array
				double dist_from=Double.parseDouble(in_node_info[1]);//Get the distance
				String new_distance=(new Double(distance_from+dist_from)).toString();//Add the distances
				String newPath= in_node_info[0]+"->"+info[3];//Map the path of the node
				String Info_Updated="1;"+new_distance+";"+newPath;//Change the format(make it visited, add visited flag 1)
				context.write(new Text(in_node_info[0]),new Text(Info_Updated));//Put new values as context for the mapper
			}
			}
			}
			
		}
		}//End of the mapper
		//The Reducer begins here
		public static class STSPReducer extends Reducer<Text,Text,Text,Text>{
			private Counter update_Counter;//Initialize the counter 
			@Override
			public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
				double current_distance=new Double(InfDistance);//Initialize the current distance
				int visited=0;//Visited flag is zero
				int updated=0;//Check it was updated or not
				String current_Path="";//Initialize the Path of followed by a node
				String in_wards="";
				for(Text val:values){
					String[] info=val.toString().split(";");//Split and store values in new array
					if (info.length>3){
						double distance_node=Double.parseDouble(info[1]);//Store the distance in a variable called distance_node
						String node_Path=info[3];
						if(info.length==5){
							in_wards=info[4];
						}
						if(distance_node<current_distance){//Distance is less than current distance
							current_distance=distance_node;//Replace the current distance with the distance
							current_Path=node_Path;//Put the path followed as current path		
						}
					}else{
						double distance_node=Double.parseDouble(info[1]);
						if(distance_node<current_distance){
							current_distance=distance_node;
							visited=1;//Mark as visited
							current_Path=info[2];//Put the path
							updated=1;//Update it
							
						}
						
					}
					
				}
				if(updated==0){//If update is 0
					visited=0;//Then marked is as not visited
				}else{
					update_Counter=context.getCounter(UpdateCounter.UpdateDistance);//For iteration the counter
					update_Counter.increment(1);//Increment the counter
					
				}
				String Updated_Info="0;"+(new Double(current_distance)).toString()+";"+(new Integer(visited)).toString()+";"+ current_Path+";"+in_wards;
				context.write(key, new Text(Updated_Info));//Put updated info in context
			}
		}//End of Reducer
		//Get the output in the right format from STSP
		public static class output_of_algo extends Mapper<Object, Text, IntWritable,Text>{
			public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
				String[] info=value.toString().split(";");//Make new array from the previous output
				if(Double.parseDouble(info[1])<InfDistance){//If distance is less than Max value
					context.write(new IntWritable(Integer.parseInt(info[0].split("\t")[0])), new Text(info[1]+"\t"+info[3]));
				//Put the distance and path for that node to reach the target
							
				}
			}
		}//End of Mapper
		//Reducer starts here
		public static class output_reducer extends Reducer<IntWritable,Text,IntWritable,Text>{
			public void reduce(IntWritable key,Text values,Context context) throws IOException,InterruptedException{
				context.write(key, values);
			}
		}//End of Reducer
		public static void main(String[] args) throws Exception {//Driver starts here
			IN=args[0];
			OUT=args[1];
			Integer TargetNode=Integer.parseInt(args[2]);//Target we need to reach
			int iteration=0;//Initialize the iteration
			String input=IN;
			String output=OUT+iteration;
			boolean finished=false;
			Configuration conf = new Configuration();
			conf.set("TargetNode", TargetNode.toString());
			Job job = Job.getInstance(conf, "Adjacency List");
			job.setJarByClass(SingleTargetSP.class);
			job.setMapperClass(Adjacency_List.class);
			job.setReducerClass(Output.class);
			//job.setMapOutputKeyClass(Text.class);
			//job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			FileSystem hdfs=FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
			input=output;//Input becomes output from previous case
			iteration ++;//Increment the iteration
			output=OUT+iteration;
			while(finished==false){
				Configuration confSTSP=new Configuration();
				Job stspJob=Job.getInstance(confSTSP,"STSP");
				stspJob.setJarByClass(SingleTargetSP.class);
				stspJob.setMapperClass(STSPMapper.class);
				stspJob.setReducerClass(STSPReducer.class);
				stspJob.setOutputKeyClass(Text.class);
				stspJob.setOutputValueClass(Text.class);
				stspJob.setNumReduceTasks(1);
				FileInputFormat.addInputPath(stspJob, new Path(input));
				FileOutputFormat.setOutputPath(stspJob, new Path(output));
				stspJob.waitForCompletion(true);
				Counters counters=stspJob.getCounters();
				Counter update_Counter=counters.findCounter(UpdateCounter.UpdateDistance);
				hdfs.delete(new Path("/user/comp9313/output"+(new Integer(iteration-1)).toString()),true);
				input=output;
				iteration ++;
				output=OUT+iteration;
				if(update_Counter.getValue()==0.0){
					finished=true;
				}
			}
			Configuration output_of_graph=new Configuration();
			Job output_graph=Job.getInstance(output_of_graph,"Output");
			output_graph.setJarByClass(SingleTargetSP.class);
			output_graph.setMapperClass(output_of_algo.class);
			output_graph.setReducerClass(output_reducer.class);
			output_graph.setOutputKeyClass(IntWritable.class);
			output_graph.setOutputValueClass(Text.class);
			output_graph.setNumReduceTasks(1);
			FileInputFormat.addInputPath(output_graph, new Path(input));
			FileOutputFormat.setOutputPath(output_graph,new Path(args[1]));
			output_graph.waitForCompletion(true);
			hdfs.delete(new Path("/user/comp9313/output"+(new Integer(iteration-1)).toString()),true);
			//System.exit(job.waitForCompletion(true) ? 0 : 1);
	}//Driver ends
}






















