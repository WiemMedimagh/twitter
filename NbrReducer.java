package tn.isima.exercice;

import java.io.IOException;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class NbrReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

int summ = 0;

public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
    
	int sum =0;
    for(IntWritable x : values){
    	sum+= x.get();
    	
      
      
    	summ += sum;
    }
    
    }
    

	@Override
protected void cleanup(Context context) throws IOException, InterruptedException {
	System.out.print("le nr de like est "+summ );
    context.write(summ, new IntWritable(sum));
   
}  
}
