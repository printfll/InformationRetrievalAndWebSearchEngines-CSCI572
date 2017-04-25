package word;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	
	public static class WordMapper extends Mapper<Object, Text, Text, Text>{

	    private Text word = new Text();
	    public final Log log = LogFactory.getLog(WordMapper.class);

	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	String[] line = value.toString().split("\t");
	    	if(value.toString().length()>0&&line.length>1){


                
                String docID = line[0];
                //log.info("DOCUMENT ID:"+docID);
                Text id = new Text(docID);
                StringTokenizer itr = new StringTokenizer(line[1]);
                while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, id);
                }
	    }
		
	    }
	}
	
	public static class WordReducer extends Reducer<Text,Text,Text,Text> {
	    private Text result = new Text();
	    public final Log log = LogFactory.getLog(WordReducer.class);
	    public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	    	HashMap<String,Integer> map=new HashMap<>();
	      for (Text val : values) {
//	    	  log.info("Key:"+key.toString()+",Text:"+val.toString());
	    	  if(val.toString().length()>0){
	    		  int i=map.getOrDefault(val.toString(), 0);
	    	        map.put(val.toString(), i+1);
	    	        
	    	  }   	  
	      }
	      
	      StringBuffer sb=new StringBuffer();
	      for(String t:map.keySet()){
			  
	    		  sb.append(t+":"+map.get(t)+",");
			  
					
	      }
	      
	      result.set(sb.toString());
	      context.write(key, result); 
	    }

	}


  public static void main(String[] args) throws Exception {
	 String input= "full_data/65267884.txt";
	 String output="output";
	 File outputPath=new File(output);
	 if(outputPath.isDirectory() && outputPath.exists()){   
		 FileUtils.deleteDirectory(outputPath);  
     }
	  
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "word count");
    job.setJobName("Word Count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(WordMapper.class);
    job.setReducerClass(WordReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
}