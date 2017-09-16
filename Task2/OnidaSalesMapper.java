package task2;
/*
 * Use mapreduce to find the number of units sold in each state for Onida.
 * 
 * My code wasn't executing properly with "|" separator.
 * After showing my work to instructor, he mentioned that
 * MapReduce is know to have some problems with it, and told me
 * to replace that separator with "," 
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class OnidaSalesMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {		
		
		
		String[] line = value.toString().split(",");
		
		if (line[0].equalsIgnoreCase("Onida") && !line[1].equalsIgnoreCase("NA")){
			//Write index 3 (State name) and 1 count to represent 1 sale.
			context.write(new Text(line[3]), new IntWritable(1));//Write this record
		}
		
	}
	
}