
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;


public class InMemory_Join {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        HashMap<String, String[]> userMap = new HashMap<String, String[]>();
        private ArrayList<String> friend_list1 = new ArrayList<>();
        String userValue = "";

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String user1 = context.getConfiguration().get("user1");
            String user2 = context.getConfiguration().get("user2");

            String[] users = value.toString().split("\t");
            String user_id = users[0];


            if (user_id.equals(user1) || user_id.equals(user2)) {

                if (friend_list1.isEmpty()) {
                    friend_list1.addAll(Arrays.asList(users[1].split(",")));
                } else {
                    String[] user_friends = users[1].split(",");
                    for (String friend : user_friends) {
                        if (friend_list1.contains(friend)) {
                            userValue = userValue + (userMap.get(friend)[0] + ":" + userMap.get(friend)[1] + ", ");
                        }
                    }
                }

                if (!userValue.equals(""))
                    context.write(new Text(user1 + "," + user2), new Text("[" + userValue.substring(0, userValue.length() - 2) + "]"));
            }
        }


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            Configuration conf = context.getConfiguration();
            Path part = new Path(conf.get("user_data"));
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fsstatus = fs.listStatus(part);

            for (FileStatus stats : fsstatus) {
                Path pt = stats.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] arr = line.split(",");
                    String[] temp = new String[2];
                    temp[0] = arr[1];
                    temp[1] = arr[9];
                    userMap.put(arr[0], temp);
                    line = br.readLine();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Validate no.of args else exit
        if (otherArgs.length != 5) {
            System.err.println("Usage: InMemory_Join <user_1> <user_2> <soc-LiveJournal1Adj.txt_path> <userdata_path> <userdata_output_path>");
            //For instance InMemory_Join 10 30 soc_file.txt userdata.txt output_path
            System.exit(2);
        }

        conf.set("user1", otherArgs[0]);
        conf.set("user2", otherArgs[1]);
        conf.set("user_data", otherArgs[3]);

        // create a job with name "InMemory_Join_mapper"
        Job job = new Job(conf, "InMemory_Join");
        job.setJarByClass(InMemory_Join.class);
        job.setMapperClass(Map.class);

        //set output format
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set file format
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));

        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}