import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Phase1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	if (value.charAt(0) != '#') {
        	String[] nodes = value.toString().split("\\t");
            String nodeFrom = nodes[0];
            String nodeTo = nodes[1];
            context.write(new Text(nodeFrom), new Text(nodeTo));
        }
 
    }
    
}