package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Random;


public class MeansElection {

    public static class MeansElectionMapper extends Mapper<LongWritable, Text, IntWritable, Point> {

        final static Random rand = new Random(42);
        final static IntWritable outputKey = new IntWritable();
        static Point outputValue;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int N = Integer.parseInt(conf.get("n"));

            outputKey.set(rand.nextInt(N));
            outputValue = Point.parse(value.toString());
            context.write(outputKey, outputValue);
        }
    }

    public static class MeansElectionReducer extends Reducer<IntWritable, Point, IntWritable, Point>{

        static int meansCount;

        public void setup(Context context){
            meansCount = 0;
        }

        public void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int K = Integer.parseInt(conf.get("k"));

            for (Point p: values){
                if (meansCount < K){
                    context.write(key, p);
                    meansCount++;
                }
            }
        }
    }

    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        // Set JAR class
        job.setJarByClass(MeansElection.class);

        // Set Mapper class
        job.setMapperClass(MeansElectionMapper.class);

        // Set Combiner class
        job.setCombinerClass(MeansElectionReducer.class);

        // Set Reducer class
        job.setReducerClass(MeansElectionReducer.class);

        // Set key-value output format
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Point.class);

        // Define input and output path file
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("intermediateOutput")));

        // Exit
        return job.waitForCompletion(true);
    }
}
