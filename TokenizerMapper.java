package tn.isima.exercice;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class TokenizerMapper
extends Mapper<LongWritable, Text, Text, NullWritable>{
@Override
public void map(NullWritable key, Text value, Context context
             ) throws IOException, InterruptedException {
	String line = value.toString();

	String[] data=line.split(",");
	try {
		String influenceur = data[0];
		int nbre_like = Integer.parseInt(data[8]);	
		
		context.write(new Text(influenceur),new IntWritable(nbre_like));
			
	} catch(Exception e ) {
		
	}
	
		
	

}
}
