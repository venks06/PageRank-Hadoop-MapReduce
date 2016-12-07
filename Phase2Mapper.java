import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Phase2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /* 
         * Input 
         * <PageA>    <page-rank of PageA>    PageX,PageY,PageZ,...
         * Output 
         * <PageA>   |PageX,PageY,PageZ,...
         * <PageX>    <page-rank of PageA >    PageX,PageY,PageZ,...
         */
    	String[] tabSeparatedValues = value.toString().split("\\t");    	
    	String page = tabSeparatedValues[0];
    	String pageRank = tabSeparatedValues[1];
    	String links = tabSeparatedValues[2];        
        String[] outerLinks = links.split(",");
        for (String outerLink : outerLinks) { 
            Text pageRankAndOuterLinksLength = new Text(pageRank + "\t" + outerLinks.length);
            context.write(new Text(outerLink), pageRankAndOuterLinksLength); 
        }        
        context.write(new Text(page), new Text(PageRank.LINKS_SEPARATOR + links));
    }
    
}