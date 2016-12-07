import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Phase1Reducer extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        /* 
         * Output <PageA>    <page-rank of PageA>    PageX,PageY,PageZ,...
         */
        
        boolean first = true;
        String links = 1 + "\t";
        for (Text value : values) {
            if (!first) 
                links += ",";
            links += value.toString();
            first = false;
        }

        context.write(key, new Text(links));
    }

}