package comp9313.project1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Class for the Average length of incoming edges
public class EdgeAvgLen1 {

	//Mapper class begins here
	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, DoublePair> {
		private Text line = new Text();
		private DoublePair weightSet = new DoublePair();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				// String data = itr.nextToken();
				line.set(itr.nextToken());
				String graph_line = line.toString();//convert the line to string
				String[] graph = graph_line.split(" ");//Convert the line to an array
				IntWritable node = new IntWritable(Integer.parseInt(graph[2]));//We consider the second term of the string as node
				//Text node_val = new Text(graph[2]);
				DoubleWritable weight = new DoubleWritable(Double.parseDouble(graph[3]));//The weight of the incoming node is a double writable field
				double w=weight.get();
				//double initial=1.0;
				//DoubleWritable initial_count=new DoubleWritable(initial);
				weightSet.set(w,1.0);
				context.write(node, weightSet);//Make a double pair with weight and count
				System.out.println(context);
				
			}
		}//End of mapper
	}
	//Combiner class beings here
	public static class Combiner extends Reducer<IntWritable, DoublePair, IntWritable, DoublePair> {
		//Pair result_reducer = new Pair();
		public void reduce(IntWritable key, Iterable<DoublePair> values, Context context) throws IOException, InterruptedException {
			DoublePair result_reducer = new DoublePair();
			double sum_weights = 0.0;
			double count = 0.0; //For all values in got using mapper find the total weight and count
			for (DoublePair val : values) {
				sum_weights += val.getFirst();
				count += val.getSecond();
			}
			//DoubleWritable s_weights = new DoubleWritable(sum_weights);
			//DoubleWritable c=new DoubleWritable(count);
			result_reducer.set(sum_weights, count);//Set the weight and count in double pair
			context.write(key, result_reducer);//Put the values of weight and count along with keys
			System.out.println(context);
		}
	}//End of combiner
//Start of reducer
	public static class EdgeAvgReducer extends Reducer<IntWritable, DoublePair, IntWritable, DoubleWritable> {
		private DoubleWritable final_answer = new DoubleWritable();
		//For values passed by combiner iterate it
		public void reduce(IntWritable key, Iterable<DoublePair> values, Context context) throws IOException, InterruptedException {
			double sum_weights = 0.0;
			double count = 0;
			for (DoublePair val : values) {
				sum_weights = val.getFirst();//Get weights for each key
				count = val.getSecond();//Get the final count
				
			}
			final_answer.set((double) sum_weights / (double) count);//Average calculated
			context.write(key, final_answer);//Average put as final answer
		}

	}//End of reducer
	//Driver class starts here
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Edge Average");
		job.setJarByClass(EdgeAvgLen1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(EdgeAvgReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoublePair.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}//Driver class ends here
	//Custom class of Double Pair created with both double values
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

	}//End of custom class

}//End of MapReduce class