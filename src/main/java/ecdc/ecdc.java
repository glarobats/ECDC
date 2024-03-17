package ecdc;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
        job.setMapperClass(FirstMap.class);
        job.setReducerClass(FirstReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }


    public static class FirstMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private HashMap<String, HashMap<String, IntWritable>> dataMap = new HashMap<>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Παράβλεψη της πρώτης γραμμής
            if (key.get() == 0) {
                return;
            }
            // Χωρίζει το κείμενο σε γραμμές
            String[] line = value.toString().split(",");
            // Λήψη συγκεκριμένων δεδομένων από τη γραμμή
            String month = line[2];
            int cases = Integer.parseInt(line[4]);
            String country = line[6];

            // Δημιουργία νέου χάρτη αν δεν υπάρχει ήδη για τη χώρα
            if (!dataMap.containsKey(country))
                dataMap.put(country, new HashMap<>());

            // Προσθήκη του αριθμού κρουσμάτων για τον συγκεκριμένο μήνα
            dataMap.get(country).put(month, new IntWritable(cases));

            // Εκπομπή του κλειδιού-τιμής (country, cases)
            context.write(new Text(country + " " + month), new IntWritable(cases));        }
    }




    public static class FirstReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;

            // Calculate the sum of cases and count the number of months
            for (IntWritable value : values) {
                sum += value.get();
                count++;
            }

            // Calculate the average
            double average = (double) sum / count;

            // Emit the country and its average number of cases per month
            context.write(key, new DoubleWritable(average));
        }
    }
}
