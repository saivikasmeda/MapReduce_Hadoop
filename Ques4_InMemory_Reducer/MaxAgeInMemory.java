import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;



public class MaxAgeInMemory {

    public static class MaxAge_MapClass extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] friends = values.toString().split("\t");
            if (friends.length == 2) {
                context.write(new Text(friends[0]), new Text(friends[1]));
            }
        }
    }

    public static class MaxAge_Reducer extends Reducer<Text, Text, Text, Text> {

        private int calculateFriendAge(String s) throws ParseException {

            Calendar today = Calendar.getInstance();
            SimpleDateFormat dataFormat = new SimpleDateFormat("MM/dd/yyyy");
            Date date = dataFormat.parse(s);
            Calendar dob = Calendar.getInstance();
            dob.setTime(date);

            int curYear = today.get(Calendar.YEAR);
            int dobYear = dob.get(Calendar.YEAR);
            int age = curYear - dobYear;

            int curMonth = today.get(Calendar.MONTH);
            int dobMonth = dob.get(Calendar.MONTH);
            if (dobMonth > curMonth) {
                age--;
            } else if (dobMonth == curMonth) {
                int curDay = today.get(Calendar.DAY_OF_MONTH);
                int dobDay = dob.get(Calendar.DAY_OF_MONTH);
                if (dobDay > curDay) {
                    age--;
                }
            }
            return age;
        }

        static HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();

        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path part = new Path(conf.get("UserData"));
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fsstatus = fs.listStatus(part);
            for (FileStatus status : fsstatus) {
                Path pt = status.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] info = line.split(",");
                    if (info.length == 10) {
                        try {
                            int age = calculateFriendAge(info[9]);
                            map.put(Integer.parseInt(info[0]), age);
                        } catch (ParseException error) {
                            error.printStackTrace();
                        }
                    }
                    line = br.readLine();
                }
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text tuples : values) {
                String[] friendsList = tuples.toString().split(",");
                int maxAge = 0;
                int age;
                for (String eachFriend : friendsList) {
                    age = map.get(Integer.parseInt(eachFriend));

                    if(age > maxAge)
                    {
                        maxAge = age;
                    }
                }
                context.write(key, new Text(Integer.toString(maxAge)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: <soc-LiveJournal1Adj.txt_path> <userdata.txt_path> <Final_output_Path>");
            System.exit(2);
        }
        conf1.set("UserData", otherArgs[1]);
        Job job = Job.getInstance(conf1, "Maximum Age");

        job.setJarByClass(MaxAgeInMemory.class);
        job.setMapperClass(MaxAge_MapClass.class);
        job.setReducerClass(MaxAge_Reducer.class);

        // set mapper output format
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //set output format
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set file format
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}