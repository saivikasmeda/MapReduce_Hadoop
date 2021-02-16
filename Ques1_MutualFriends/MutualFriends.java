import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;


public class MutualFriends {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        Text user = new Text();
        Text friends = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\t");

            String user_id = split[0];
            if (split.length == 1) {
                return;
            }

            String[] others = split[1].split(",");
            for (String friend : others) {

                if (user_id.equals(friend))
                    continue;

                String userKey = (Integer.parseInt(user_id) < Integer.parseInt(friend)) ? user_id + "," + friend : friend + "," + user_id;
                String regex = "((\\b" + friend + "[^\\w]+)|\\b,?" + friend + "$)";

                friends.set(split[1].replaceAll(regex, ""));
                user.set(userKey);
                context.write(user, friends);
            }
        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private String findMutualFriends(String friends1, String friends2) {

            if (friends1 == null || friends2 == null)
                return null;

            String[] friendsList1 = friends1.split(",");
            String[] friendsList2 = friends2.split(",");

            LinkedHashSet<String> friendSet1 = new LinkedHashSet<>();
            Collections.addAll(friendSet1, friendsList1);

            LinkedHashSet<String> friendSet2 = new LinkedHashSet<>();
            Collections.addAll(friendSet2, friendsList2);

            //keep only the matching items in set1
            friendSet1.retainAll(friendSet2);

            return friendSet1.toString().replaceAll("[\\[]]", "");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] friendsList = new String[2];
            int i = 0;

            for (Text value : values) {
                friendsList[i++] = value.toString();
            }
            String mutualFriends = findMutualFriends(friendsList[0], friendsList[1]);
            if (mutualFriends != null && mutualFriends.length() != 0) {
                context.write(key, new Text(mutualFriends));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Check input arguments
        if (otherArgs.length != 2) {
            System.err.println("Usage: mutualFriends.jar <input soc-LiveJournal1Adj.txt> <output file path>");
            System.exit(2);
        }

        // create a job with name "MutualFriends"
        Job job = new Job(conf, "MutualFriends");

        //Set Class format
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //Set Output Format
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //File Format
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}