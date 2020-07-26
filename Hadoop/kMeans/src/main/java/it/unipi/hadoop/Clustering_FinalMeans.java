package it.unipi.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Clustering_FinalMeans {
        
    public static class Clustering_FinalMeansMapper extends Mapper<LongWritable, Text, Point, AccumulatorPoint> {
        private static final Point meanPoint = new Point();
        private static final Point dataPoint = new Point();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] meanAndDataString = value.toString().split("\t");
            AccumulatorPoint partialNewMean = new AccumulatorPoint();
            
            meanPoint.set(Point.parse(meanAndDataString[0]));
            dataPoint.set(Point.parse(meanAndDataString[1]));
            partialNewMean.add(dataPoint);
            
            context.write(meanPoint, partialNewMean);
        }   
    }
    
    public static class Clustering_FinalMeansCombiner extends Reducer<Point, AccumulatorPoint, Point, AccumulatorPoint> {

        public void reduce(Point key, Iterable<AccumulatorPoint> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            AccumulatorPoint partialNewMean = new AccumulatorPoint();
            
            for(AccumulatorPoint partialMean : values)
                partialNewMean.add(partialMean);

            context.write(key, partialNewMean);
        }
    }
    
    public static class Clustering_FinalMeansReducer extends Reducer<Point, AccumulatorPoint, NullWritable, Point> {
        private static final Point newMean = new Point();
        
        public void reduce(Point key, Iterable<AccumulatorPoint> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            AccumulatorPoint partialNewMean = new AccumulatorPoint();
            
            for(AccumulatorPoint partialMean : values)
                partialNewMean.add(partialMean);
            
            // Id is the same of the relative mean point.  
            newMean.set(partialNewMean.getValue().getCoordinates(), PointType.MEAN, key.getId());
            newMean.div(partialNewMean.getNumberOfPoints());
            
            context.write(null, newMean);
        }
    }
    
    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        // Set JAR class.
        job.setJarByClass(Clustering_FinalMeans.class);

        // Set Mapper class.
        job.setMapperClass(Clustering_FinalMeansMapper.class);

        // Set Combiner class.
        job.setCombinerClass(Clustering_FinalMeansCombiner.class);

        // Set Reducer class. There can be multiple reducers.
        job.setReducerClass(Clustering_FinalMeansReducer.class);
        job.setNumReduceTasks(conf.getInt("clusteringNumberOfReduceTasks", 1));
        
        // Set key-value output format.
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(AccumulatorPoint.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Point.class);
        
        // Define input and output path file. 
        FileInputFormat.addInputPath(job, new Path(conf.get("clusteringClosestPoints")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("clusteringFinalMeans")));
        
        // Exit.
        return job.waitForCompletion(conf.getBoolean("verbose", true));
    } 
}
