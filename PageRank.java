import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class PageRank {
    
	public static Double Damping_Factor = 0.85;
    public static String LINKS_SEPARATOR = "|";
    
    public static void main(String[] args) throws Exception {     
    	String inputPath = args[0];
    	String outputPath = args[1];
    	Integer iterations = Integer.parseInt(args[2]);
    	
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(outputPath)))
            fs.delete(new Path(outputPath), true);
        
        String newInPath = null;;
        String newOutPath = null;
        PageRank pagerank = new PageRank();
        
        System.out.println("Starting Phase1 - Initial Rank calculation");
        boolean isCompleted = pagerank.phase1(inputPath, outputPath + "/iter0");
        if (!isCompleted) {
            System.exit(1);
        }
        System.out.println("Phase1 - completed");
		
        for (int runs = 0; runs < iterations; runs++) {
        	newInPath = outputPath + "/iter" + runs;
        	newOutPath = outputPath + "/iter" + runs + 1;
            System.out.println("############# Starting Phase2 for "+(runs + 1)+" "+ iterations +" - New PageRank calculations");
            isCompleted = pagerank.phase2(newInPath, newOutPath);
            if (!isCompleted) {
                System.exit(1);
            }
        }
        System.out.println("Phase2 - completed");
		
        System.out.println("############# Starting Phase3 ---- final rank ordering Phase ");
        isCompleted = pagerank.phase3(newOutPath, outputPath + "/result");
        if (!isCompleted) {
            System.exit(1);
        }
        System.out.println("Phase3 - completed");
		
        System.out.println("############# All phases ran successfully !!!");
        System.exit(0);
    }
    
    
    public boolean phase1(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {        
        Job job = Job.getInstance(new Configuration(), "Phase-1");
        job.setJarByClass(PageRank.class);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Phase1Mapper.class);
        job.setReducerClass(Phase1Reducer.class);
        return job.waitForCompletion(true);     
    }
    
   
    public boolean phase2(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {        
        Job job = Job.getInstance(new Configuration(), "Phase-2");
        job.setJarByClass(PageRank.class);
        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(Phase2Mapper.class);
        job.setReducerClass(Phase2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true);        
    }
    
    public boolean phase3(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        Job job = Job.getInstance(new Configuration(), "Phase-3");
        job.setJarByClass(PageRank.class);
        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(Phase3Mapper.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true);
    }
    
}
