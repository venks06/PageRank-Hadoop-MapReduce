import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Phase2Reducer extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
                                                                                InterruptedException {
        /* Input
         * <PageA>   |PageX,PageY,PageZ,...
         * <PageX>    <page-rank of PageA >    PageX,PageY,PageZ,...
         */
        String links = "";
        double SharedSumOfOtherPageRanks = 0.0;
        for (Text value : values) {
            String content = value.toString();
            if (content.startsWith(PageRank.LINKS_SEPARATOR)) {
                links += content.substring(PageRank.LINKS_SEPARATOR.length());
            } else {
                String[] split = content.split("\\t");
                double pageRank = Double.parseDouble(split[0]);
                int totalLinks = Integer.parseInt(split[1]);
                SharedSumOfOtherPageRanks += (pageRank / totalLinks);
            }
        }
        double newRank = PageRank.Damping_Factor * SharedSumOfOtherPageRanks + (1 - PageRank.Damping_Factor);
        context.write(key, new Text(newRank + "\t" + links));
    }

}