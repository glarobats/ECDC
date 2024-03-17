package ecdc;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ecdc {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "ncdc");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
      //  job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }


    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text data = new Text();


        // Αναλύει το κείμενο και δημιουργεί νέο πίνακα
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the CSV line by comma
            String[] line = value.toString().split(",");

            if (line.length > 0) {
                List<String> dataList = new ArrayList<>();

                // Iterate over the elements of the CSV line
                for (int i = 0; i < line.length; i++) {
                    // Skip the first element
                    if (i == 0) {
                        continue;
                    }
                    dataList.add(line[i].trim()); // Add each element to the dataList
                }

                // Print the dataList
                System.out.println(dataList);
            }
        }
    }


    /*
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            // Αθροίζει τα n-grams
            for (IntWritable val : values)
                sum += val.get();

            // Επιστρέφει το n-gram και το άθροισμα
            context.write(key, new IntWritable(sum));
        }
    }

         */
}
