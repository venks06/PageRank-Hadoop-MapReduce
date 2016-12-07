import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Phase3Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* Rank Ordering(mapper only)
         * Input <PageA>    <page-rank of PageA>    PageX,PageY,PageZ,...
         */
        
        String[] tabSeparatedValues = value.toString().split("\\t");	
    	String page = tabSeparatedValues[0];
    	String pageRankString = tabSeparatedValues[1];
        float pageRank = Float.parseFloat(pageRankString);
        context.write(new DoubleWritable(pageRank), new Text(page));
    }
       
}