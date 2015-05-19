
// here we are importing library

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;	
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Writing mapper class

public class Mapr_sd_example {
	
	 public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{

// Writing Map Function
		 
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	 
//****************************************************************	 
//************ You need to edit below for mapper *****************	 
//****************************************************************	 
	 
	 
	 
 	String[] split = value.toString().split(",");
	context.write(new Text(split[0]), new IntWritable(Integer.parseInt(split[2])));
	
	
	
	
	
	
	
//****************************************************************		
//***************You need to edit above for mapper ***************
//****************************************************************		
	
	
 }
}
	 
	 // Writing Reducer Class

public static class IntSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result ;
 
 //Writing Reducer Function

 public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
	 
	 
	 
//****************************************************************		 
//************* you need to edit below for Reducer ***************
//****************************************************************		 
	 
	 
	 
	 
	 
   int sum = 0 ;
   for (IntWritable val : values) {
	   
 	  sum += val.get(); 
 	  
   }

   result = new IntWritable(sum);
   
   context.write(key, result);
   
   
   
   
   
   
   
   
//****************************************************************	  
// ********** you need to edit above for Reducer ***************** 
//****************************************************************	  
 }
}

	
	
	// Writing main function to set configuration and driver class 

public static void main(String[] args) throws Exception {
   Configuration conf = new Configuration();
   conf.set("index","4"); 
   Job job = Job.getInstance(conf, "fso_regression_example_mapr");
   job.setJarByClass(Mapr_sd_example.class);
   job.setMapperClass(TokenizerMapper.class);
   job.setReducerClass(IntSumReducer.class);
   job.setMapOutputKeyClass(Text.class);
   job.setMapOutputValueClass(IntWritable.class);
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(IntWritable.class);
   FileInputFormat.addInputPath(job, new Path(args[0]));
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
   System.exit(job.waitForCompletion(true) ? 0 : 1);
 }


}
