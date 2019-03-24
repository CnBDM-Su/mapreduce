import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
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
public class dist{
    public String[] ex = new String[10];
    public static class WordCountMapper extends Mapper<Object, Text, Text, Text>{
      //  private final static IntWritable plugone = new IntWritable(1);
        private Text word = new Text();
        private Text file = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
            StringTokenizer st = new StringTokenizer(value.toString()," !.:;()?&,'-");
            int Sum = 0;
            int num = 0;
            String vac = "";
            int[] mid = new int[100];
            IntWritable[] vec = new IntWritable[100];
            IntWritable mmid = new IntWritable();
            aa:while (st.hasMoreTokens()){
                word.set(st.nextToken().toLowerCase());
                vac = word.toString();
                if(vac.startsWith("ex")){
                    file.set(filename);
                    context.write(word, file);
                }

            
        }
    }
}
 
    public static class MapUtil{
        public static <K,V extends Comparable<? super V>> List<Map.Entry<K,V>> sortValue(Map<K,V> map){
            List<Map.Entry<K,V>> list = new ArrayList(map.entrySet());
       //     list.sort(Map.Entry.comparingByValue());
      //      Map<String, Integer> result = new LinkedHashMap<>();
      //      for(Map.Entry<String,Integer> entry:list){
        //        result.put(entry.getKey(),entry.getValue());
        //    } 
        //    return result;

            Collections.sort(list,
      //    SortedSet<Map.Entry<K,V>> result = new TreeSet<Map.Entry<K,V>>(
            new Comparator<Map.Entry<K,V>>() {
                @Override
                public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2){
                    int res = e2.getValue().compareTo(e1.getValue());
                    if(e2.getValue()==null && e1.getValue()==null){
                        return 0;
                    }
                    else {return res;} 
        
          }});
            return list;
         //   }});
          //  return list;
    }}
    
    public static class WordCountReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String,String> mapp = new TreeMap<String,String>();
        private Map<String,Integer> mmm = new TreeMap<String,Integer>();
        private Map<String, String> mapsort = new LinkedHashMap<String,String>();
        private Text file = new Text(); 
        private Text result = new Text(); 
        private Text result1 = new Text();  
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            String[] strings = new String[11];
            String[] filename = new String[11];
            int[] mid = new int[10];
            for(Text val:values){
                sum++;
                int num = 0;
                for(int i=0;i<9;i++){
                    if(val.toString().equals("file0"+String.valueOf(i+1)+".txt")){
                        mid[i] += 1;
                        break;}
                    else{
                        if(val.toString().equals("file10.txt")){
                            mid[9] +=1;
                            break;}
                    }
                } }
                for(int i = 0; i< 10 ;i++){
                    strings[i] = String.valueOf(mid[i]); 
                    }
                strings[10] =  String.valueOf(sum);
                for(int i =0;i<9;i++){
                    filename[i] = "file0"+String.valueOf(i+1)+".txt"+key.toString();
                }
                filename[9]="file10.txt"+key.toString();
                filename[10]="Total .txt"+key.toString();
                for(int i =0;i<11;i++){
                    mapp.put(filename[i],strings[i]);}
                
            }
    public void cleanup(Context context) throws IOException, InterruptedException{     
     //   MyComparator comparator = new MyComparator(mapp);
      //  Stream<Map.Entry<String,String>> mapsort = mapp.entrySet().stream().sorted(Map.Entry.comparingByValue(comparator));
        for(Map.Entry<String,String> entry:mapp.entrySet()){
            mmm.put(entry.getKey(),Integer.valueOf(entry.getValue()));
      }      
        List<Map.Entry<String,Integer>> result = MapUtil.sortValue(mmm);
    
        for(Map.Entry<String,Integer> entry1:result){
            mapsort.put(entry1.getKey(),String.valueOf(entry1.getValue()));
      } 
  
      String[] strings = new String[11];
        for(int i=0;i<11;i++){
            strings[i]=" ";
            }
        int[] mid = new int[10];
        String filename = " ";
     /*       Set<Map.Entry<Text,Text>> entrySet = map.entrySet();
            Iterator<Map.Entry<Text,Text>> it = entrySet.iterator();
            while(it.hasNext()){
                Map.Entry<Text,Text> next = it.next();
   */       for(Map.Entry<String,String> entryset : mapsort.entrySet()){
                String key = entryset.getKey();
                String value = String.valueOf(entryset.getValue());
                String key1 = key.substring(0,10);
                String key2 = key.substring(10);
                for(int i=0;i<10;i++){
                if(key1.equals("file0"+String.valueOf(i+1)+".txt")){
                    strings[i] = strings[i]+key2+","+value+",";
                    break;
                }
                else{
                    if(key1.equals("file10.txt")){
                        strings[9] = strings[9]+key2+","+value+",";
                        break;}
                    if(key1.equals("Total .txt")){
                        strings[10] = strings[10]+key2+","+value+",";
                        break;
                    }
            }
          }}
      
        
            for(int i=0;i<9;i++){
                result1.set(strings[i]);
                filename = "file0"+String.valueOf(i+1)+".txt";
                file.set(filename);
                context.write(file,result1);  
                }
            result1.set(strings[9]);
            filename = "file10.txt";
            file.set(filename);
            context.write(file,result1);
            result1.set(strings[10]);
            filename = "Total.txt";
            file.set(filename);
            context.write(file,result1);
                }
        }


              
    

    public static void main(String[] args) throws Exception {
        String[] input = new String[10];
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Bag of Word");
    //    job.setJar("/Users/64161/Desktop/testFiles/1.jar");
        job.setJarByClass(dist.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapperClass(WordCountMapper.class);
   //     job.setCombinerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
   //     job.getConfiguration().setStrings("mapreduce.reduce.shuffle.memory.limit.percent","0.15");
  //      for(int d=0; d<9;d++){
  //          input[d] = "hdfs://localhost:9000/file0"+String.valueOf(d+1)+".txt";
    //        FileInputFormat.addInputPath(job, new Path(input[d]));
     //       }
    //    input[9] = "hdfs://localhost:9000/file10.txt";
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    }

    