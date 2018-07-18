package comp9313.project1;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import comp9313.project1.EdgeAvgLen1.Combiner;
//import comp9313.project1.EdgeAvgLen1.DoublePair;
//import comp9313.project1.EdgeAvgLen1.EdgeAvgReducer;
//import comp9313.project1.EdgeAvgLen1.TokenizerMapper;
//Class for Map Reduce starts here
public class EdgeAvgLen2 {
	//Custom class for Double Pair created here with both elements double
	public static class DoublePair implements Writable {
		private double first,second;
		

		public DoublePair() {
			
		}

		public DoublePair(double first, double second) {
			set(first, second);
		}

		public void set(double left, double right) {
			first = left;
			second = right;
		}

		public double getFirst() {
			return first;
		}

		public double getSecond() {
			return second;

		}

		public void write(DataOutput out) throws IOException {
			out.writeDouble(first);
			out.writeDouble(second);

		}

		public void readFields(DataInput in) throws IOException {
			first=in.readDouble();
			second=in.readDouble();
		}

	}//End of Custom class
	//Mapper begins here
	public static class TokenizerMapper extends Mapper<Object,Text,IntWritable,DoublePair>{
		private String line;
		private Map<IntWritable,Double> weight=new HashMap<IntWritable,Double>();
		private Map<IntWritable,Double> count= new HashMap<IntWritable,Double>();
		private DoublePair weightset=new DoublePair();
		
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while(itr.hasMoreTokens()){
				line=itr.nextToken();
				String graph_line = line.toString();//Convert input to string
				String[] graph = graph_line.split(" ");//Convert String to array
				IntWritable first_node=new IntWritable(Integer.parseInt(graph[2]));//Node is the second element
				double weight_of_node=Double.parseDouble(graph[3]);
				if(weight.containsKey(first_node)){//For key value find weight
					double total=(double) weight.get(first_node)+weight_of_node;//If key found add the next weight found
					weight.put(first_node, total);
				} else{
					weight.put(first_node, weight_of_node);//Else add the initial weight
				}
				if(count.containsKey(first_node)){//For every key do the count
					double total_count=(double) count.get(first_node)+1.0;//Increment count if key found
					count.put(first_node, total_count);
				}else{
					count.put(first_node, 1.0);//Else keep it as one
				}
				}
				
					}
		//End of map
		public void cleanup(Context context) throws IOException,InterruptedException{//Do the combining here for every key
			for(IntWritable first_node:weight.keySet()){
				double node_weight=weight.get(first_node);
				double count_node=count.get(first_node);
				weightset.set(node_weight, count_node);
				context.write(first_node, weightset);
				
			}
		}//End of cleanup class
	}//End of Mapper
	//Reducer starts from here
		public static class EdgeAvgReducer extends Reducer<IntWritable,DoublePair,IntWritable,DoubleWritable>{
			private DoubleWritable final_answer = new DoubleWritable();
			public void reduce(IntWritable key, Iterable<DoublePair> values, Context context) throws IOException, InterruptedException {
				double sum_weights = 0.0;
				double count = 0.0;
				for (DoublePair val : values) {
					sum_weights = val.getFirst();//for all key get total weights
					count = val.getSecond();	//For all key get total count
				}
				final_answer.set((double) sum_weights / (double) count);//Find average
				context.write(key, final_answer);//Put the final answer with key value
			}
		}//End of Reducer
		//Driver begins here
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Incoming Edge Average");
			job.setJarByClass(EdgeAvgLen2.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(EdgeAvgReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(DoublePair.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}//End of Driver 
		
	
	

}//End of MapReduce class
