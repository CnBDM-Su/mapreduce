import java.util.StringTokenizer;
import java.util.Vector;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class bow{
    public static class WordCountMapper extends Mapper<Object, Text, Text, Text>{
      //  private final static IntWritable plugone = new IntWritable(1);
        private Text word = new Text();
        private Text file = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
            StringTokenizer st = new StringTokenizer(value.toString()," !.:;()?&,'-");
            int num = 0;
            int[] mid = new int[100];
            IntWritable[] vec = new IntWritable[100];
            IntWritable mmid = new IntWritable();
            aa:while (st.hasMoreTokens()){
                word.set(st.nextToken().toLowerCase());
                file.set(filename);
                context.write(file, word);
            
        }
    }
}

   /*     public static class IntArrayWritable extends ArrayWritable{
            public IntArrayWritable(){
                super(IntWritable.class);
            }
            public IntArrayWritable(Writable[] values){
                super(IntWritable.class, values);
            }
            public Writable[] get(){
                return (Writable[]) super.get();
            }
            
            @Override
            public String toString()
            {
                Writable[] mm = get();
                IntWritable[] values = (IntWritable[]) mm;
                String strings =" ";
                for(int i = 0; i< values.length;i++){
                    strings = strings+ ","+values[i].toString(); 
                }
                return strings;
            }     
    }

      */
    public static class WordCountReducer extends Reducer<Text, Text, Text, Text> {
        private final static String[] top100Word = { "the", "be", "to", "of", "and", "a", "in", "that", "have", "i",
        "it", "for", "not", "on", "with", "he", "as", "you", "do", "at", "this", "but", "his", "by", "from",
        "they", "we", "say", "her", "she", "or", "an", "will", "my", "one", "all", "would", "there", "their",
        "what", "so", "up", "out", "if", "about", "who", "get", "which", "go", "me", "when", "make", "can",
        "like", "time", "no", "just", "him", "know", "take", "people", "into", "year", "your", "good", "some",
        "could", "them", "see", "other", "than", "then", "now", "look", "only", "come", "its", "over", "think",
        "also", "back", "after", "use", "two", "how", "our", "work", "first", "well", "way", "even", "new",
        "want", "because", "any", "these", "give", "day", "most", "us" };
        private Text result = new Text();  
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            int Sum = 0;
            String strings =" ";
            int[] mid = new int[100];
			for(int i =0;i<100;i++){
			mid[i] = 0;}
            IntWritable[] vec = new IntWritable[100];
            IntWritable mmid = new IntWritable();
            for(Text val:values){
                int num = 0;
                for(int i=0;i<100;i++){
                    if(val.toString().equals(top100Word[i])){
                        num = i;
                        break;}
                    else{
                        num = 100;
                    }
                }
                if(num < 100){
                    mid[num] += 1;}}  
                for(int i = 0; i< 99 ;i++){
                    strings = strings+String.valueOf(mid[i])+","; 
                    }
                strings = strings+String.valueOf(mid[99]); 
         
       result.set(strings);
            context.write(key, result);
        } 
    }
            
    

    public static void main(String[] args) throws Exception {
        String[] input = new String[10];
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Bag of Word");
  //      job.setJar("/Users/64161/Desktop/testFiles/1.jar");
        job.setJarByClass(bow.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapperClass(WordCountMapper.class);
 //       job.setCombinerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 //       job.getConfiguration().setStrings("mapreduce.reduce.shuffle.memory.limit.percent","0.15");
  //      for(int d=0; d<9;d++){
 //           input[d] = "hdfs://localhost:9000/file0"+String.valueOf(d+1)+".txt";
   //         FileInputFormat.addInputPath(job, new Path(input[d]));
    //        }
    //    input[9] = "hdfs://localhost:9000/file10.txt";
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    }