
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.util.Comparator;
import java.util.Map.Entry;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;


public class MaxCommonFriends {

    public static class MaxCommonFriends_Map1 extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if (line.length == 2) {
                String friend1 = line[0];
                String[] values = line[1].split(",");
                for (String friend2 : values) {
                    int frnd1 = Integer.parseInt(friend1);
                    int frnd2 = Integer.parseInt(friend2);
                    if (frnd1 < frnd2)
                        word.set(friend1 + "," + friend2);
                    else
                        word.set(friend2 + "," + friend1);
                    context.write(word, new Text(line[1]));
                }
            }
        }
    }

    public static class MaxCommonFriends_Reduce1 extends Reducer<Text, Text, Text, IntWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            int count = 0;
            for (Text friends : values) {
                String[] temp = friends.toString().split(",");
                for (String friend : temp) {
                    if (map.containsKey(friend))
                        count += 1;
                    else
                        map.put(friend, 1);
                }
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static class MaxCommonFriends_Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            {
                context.write(one, value);
            }
        }
    }

    public static class MaxCommonFriends_Reduce2 extends Reducer<IntWritable, Text, Text, IntWritable> {

        public void reduce(IntWritable key, Iterable<Text> values,
                           Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap<String, Integer>();

            int max = 0;

            for (Text line : values) {
                String[] fields = line.toString().split("\t");
                if (fields.length == 2) {
                    map.put(fields[0], Integer.parseInt(fields[1]));
                }
            }

            TreeMap<String, Integer> friends_map = new TreeMap<String, Integer>(map);
            friends_map.putAll(map);

            for (Entry<String, Integer> entry : friends_map.entrySet()) {
                if (max <= entry.getValue()) {
                    max = entry.getValue();
                }
            }

            for(Entry<String, Integer> entry: friends_map.entrySet()){
                if(max == entry.getValue()){
                    context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // getting all the arguments and if not display usage
        if (otherArgs.length != 3) {
            System.err.println("Usage: MaxCommonFriends.jar <inputfile soc-LiveJournal1Adj.txt> <temp_outputfile1 HDFS path> <final_outputfile2 HDFS path>");
            System.exit(2);
        }

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "MaxCommonFriends intermediate MutualFriends Count");
        job.setJarByClass(MaxCommonFriends.class);
        job.setMapperClass(MaxCommonFriends_Map1.class);
        job.setReducerClass(MaxCommonFriends_Reduce1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean mapreduce = job.waitForCompletion(true);

        if (mapreduce) {
            Configuration conf1 = new Configuration();
            @SuppressWarnings("deprecation")
            Job job2 = new Job(conf1, "MaxCommonFriends fetching pairs with Max count");
            job2.setJarByClass(MaxCommonFriends.class);
            job2.setMapperClass(MaxCommonFriends_Map2.class);
            job2.setReducerClass(MaxCommonFriends_Reduce2.class);
            job2.setInputFormatClass(TextInputFormat.class);

            // set map2 output format
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);

            // Set Output Format
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            // set File Format
            FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}