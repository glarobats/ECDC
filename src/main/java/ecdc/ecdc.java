package ecdc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
        Job job = new Job(conf, "ecdc");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(FirstMap.class);
        job.setReducerClass(FirstReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);


        // Προσθήκη του αρχείου εξόδου του πρώτου reducer στη cache
        DistributedCache.addCacheFile(new URI("output1/part-r-00000"), conf);

        // Ρύθμιση για τον δεύτερο κύκλο MapReduce
        Job secondJob = new Job(conf, "ecdc-second");

        // Χρήση της έξοδου του πρώτου reducer ως είσοδο για τον δεύτερο mapper
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(IntWritable.class);
        secondJob.setMapperClass(SecondMapper.class);
        secondJob.setReducerClass(SecondReducer.class);
        secondJob.setInputFormatClass(TextInputFormat.class);
        secondJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(secondJob, new Path(args[0])); // Είσοδος της δεύτερης εργασίας
        FileOutputFormat.setOutputPath(secondJob, new Path(args[2])); // Έξοδος της δεύτερης εργασίας

        // Αναμονή για την ολοκλήρωση της δεύτερης εργασίας
        secondJob.waitForCompletion(true);
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
            int cases = Integer.parseInt(line[4]);
            String country = line[6];

            // Δημιουργία νέου χάρτη αν δεν υπάρχει ήδη για τη χώρα
            if (!dataMap.containsKey(country))
                dataMap.put(country, new HashMap<>());

            // Προσθήκη του αριθμού κρουσμάτων για τον συγκεκριμένο μήνα ανάλογα με τη χώρα
            dataMap.get(country).put(country, new IntWritable(cases));

            // Εκπομπή του κλειδιού-τιμής (country, cases)
            context.write(new Text(country + " "), new IntWritable(cases));
        }

}




    public static class FirstReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;

            // Υπολογισμός του αθροίσματος και του πλήθους των τιμών
            for (IntWritable value : values) {
                sum += value.get();
                count++;
            }

            // Υπολογισμός του μέσου όρου
            double average = (double) sum / count;

            // Εκπομπή του κλειδιού-τιμής (country, average)
            context.write(key, new DoubleWritable(average));
        }
    }



    public static class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private HashMap<String, DoubleWritable> dataMap = new HashMap<>();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Πρόσβαση στο αρχείο που βρίσκεται στο configuration
            Configuration conf = context.getConfiguration();
            // Λήψη του path του αρχείου από το DistributedCache
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);

            for (Path p : cacheFiles) {
                // Ανάγνωση του αρχείου
                BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
                String line;
                while ((line = reader.readLine()) != null) {
                    // Χωρίζει το κείμενο σε γραμμές
                    String[] data = line.split(" ");
                    // Αποθήκευση των δεδομένων στον χάρτη
                    dataMap.put(data[0], new DoubleWritable(Double.parseDouble(data[1])));
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            HashMap<String, DoubleWritable> ogData = new HashMap<>();

            // Παράβλεψη της πρώτης γραμμής
            if (key.get() == 0) {
                return;
            }

            // Χωρίζει το κείμενο σε γραμμές
            String[] line = value.toString().split(",");
            // Λήψη συγκεκριμένων δεδομένων από τη γραμμή
            int cases = Integer.parseInt(line[4]);
            String country = line[6];

            // Δημιουργία του DoubleWritable αντικειμένου και αλλαγή του τύπου του cases
            DoubleWritable casesWritable = new DoubleWritable(Double.parseDouble(String.valueOf(cases)));

            // Αποθήκευση των δεδομένων στον χάρτη
            ogData.put(country, casesWritable);

            // Μεταφορά των δεδομένων στον reducer
            for (Map.Entry<String, DoubleWritable> d : dataMap.entrySet()) {
                for (Map.Entry<String, DoubleWritable> o : ogData.entrySet()) {
                    if (d.getKey().equals(o.getKey()) && o.getValue().get() > d.getValue().get())
                           context.write(new Text(o.getKey() + " "), one);
                }
            }
        }
    }



    public static class SecondReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            // Υπολογισμός του αθροίσματος των τιμών
            for (IntWritable value : values) {
                sum += value.get();
            }

            // Εκπομπή του κλειδιού-τιμής (country, sum)
            context.write(key, new IntWritable(sum));
        }
    }
}
