package ecdc;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.ArrayList;

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

            List <String> line = new ArrayList<String>();

            // Χωρίζει το κείμενο σε γραμμές
            StringTokenizer tokenizer = new StringTokenizer(value.toString());

            // Προσθέτει τις γραμμές στον πίνακα


            // Χωρίζει τα δεδομένα σε tokens χρησιμοποιώντας το tab ως διαχωριστικό
            String[] tokens = value.toString().split("\t");

            // Δημιουργεί ένα HashMap
            HashMap<String, List<String>> words = new HashMap<>();

            // Πρώτο token ως κλειδί
            String keyString = tokens[0];

            // Δημιουργεί λίστα με τα υπόλοιπα tokens
            List<String> dataList = new ArrayList<>();
            for (int i = 1; i < tokens.length; i++) {
                dataList.add(tokens[i]);
            }

            // Προσθέτει το κλειδί και την τιμή στο HashMap
            words.put(keyString, dataList);



            // Εκτύπωση του HashMap
            System.out.println(words);
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
