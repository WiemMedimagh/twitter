package tn.isima.exercice;

import java.io.IOException;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class NbrReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

int summ = 0;

public void reduce(Text key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException{
    
	int sum =0;
    for(IntWritable x : values){
    	sum+= x.get();
    	
      
    
    	summ += sum;
    	
    }
    }
    }
@Override
protected void cleanup(Context context) throws IOException, InterruptedException {
	System.out.print("tous les nbrs de like influenceur  est "+summ );
    
}
}