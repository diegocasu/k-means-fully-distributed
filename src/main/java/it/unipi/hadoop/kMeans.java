package it.unipi.hadoop;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class kMeans {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 5){
            System.err.println("Usage: hadoop jar target/kMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.kMeans <n> <d> <k> <in> <out>");
            System.exit(1);
        }

        System.out.println("n=" + otherArgs[0]);        // number of points
        System.out.println("d=" + otherArgs[1]);        // point space dimensions
        System.out.println("k=" + otherArgs[2]);        // number of means
        System.out.println("input=" + otherArgs[3]);
        System.out.println("output=" + otherArgs[4]);

        conf.set("n", otherArgs[0]);
        conf.set("d", otherArgs[1]);
        conf.set("k", otherArgs[2]);
        conf.set("input", otherArgs[3]);
        conf.set("output", otherArgs[4]);
        conf.set("startingMeans", "starting-means");
        conf.set("intermediateMeans", "intermediate-means");
        conf.set("finalMeans", "final-means");

        FileUtils.deleteDirectory(new File(conf.get("startingMeans")));

        /* Sampling means -- first map and reduce */
        Job sampling = Job.getInstance(conf, "sampling means");
        Sampling.main(sampling);

        /*
            Now we have the sampled means in the starting-means directory
        */

        int step = 0;
        double err = Double.POSITIVE_INFINITY;
        double prev_err;

        do {
            prev_err = err;
            if (step == 0) {
                /* If it's the first step we take the sampled means */
                FileUtils.copyDirectory(new File(conf.get("startingMeans")), new File(conf.get("intermediateMeans")));
            } else {
                /* In the next steps we take the new centroids computed in the previous step */
                FileUtils.copyDirectory(new File(conf.get("finalMeans")), new File(conf.get("intermediateMeans")));
            }

            /* We can get rid of previous centroids because we are going to compute new ones */
            FileUtils.deleteDirectory(new File(conf.get("finalMeans")));

            Job clustering = Job.getInstance(conf, "clustering");
            clustering.addCacheFile(new Path(conf.get("intermediateMeans") + "/part-r-00000").toUri());
            boolean clusteringExit = Clustering.main(clustering);

            FileUtils.deleteDirectory(new File(conf.get("output")));

            Job convergence = Job.getInstance(conf, "convergence");
            convergence.addCacheFile(new Path(conf.get("finalMeans") + "/part-r-00000").toUri());
            boolean convergenceExit = Convergence.main(convergence);

            File f = new File(conf.get("output")+"/part-r-00000");
            Scanner sc = new Scanner(f);

            if ( !sc.hasNextLine() ) { System.exit(1); }

            err = Double.parseDouble(sc.nextLine());
            System.out.printf("\nSTEP: %d - PREV: %f - ERR: %f - CHANGE: %.2f%%\n\n", step, prev_err, err, (prev_err - err)/prev_err * 100);

            step++;
        } while (prev_err == Double.POSITIVE_INFINITY || (prev_err - err)/prev_err > 0.01);
    }
}
