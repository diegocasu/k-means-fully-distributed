package it.unipi.hadoop;

import java.io.BufferedReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;


public class kMeans {
    private static FileSystem hdfs;
    
    private static void setupConfiguration(LocalConfiguration localConfig, Configuration conf) {
        // File system manipulation.
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());

        // Parameters.
        conf.setLong("numberOfPoints", localConfig.getNumberOfPoints());
        conf.setInt("numberOfDimensions", localConfig.getNumberOfDimensions());
        conf.setInt("numberOfClusters", localConfig.getNumberOfClusters());
        conf.set("inputPath", localConfig.getInputPath());
        conf.setInt("seedRNG", localConfig.getSeedRNG());
        conf.setInt("clusteringNumberOfReduceTasks", localConfig.getClusteringNumberOfReduceTasks());
        conf.setDouble("errorThreshold", localConfig.getErrorThreshold());
        conf.setBoolean("verbose", localConfig.getVerbose());
        
        // Working directories, based on the given output path.        
        conf.set("sampledMeans", localConfig.getOutputPath() + "/" + "sampled-means");
        conf.set("intermediateMeans", localConfig.getOutputPath() + "/" + "intermediate-means");
        conf.set("clusteringClosestPoints", localConfig.getOutputPath() + "/" + "clustering-closest-points");
        conf.set("clusteringFinalMeans", localConfig.getOutputPath() + "/" + "clustering-final-means");
        conf.set("convergence", localConfig.getOutputPath() + "/" + "convergence");
    }
    
    private static void createDirectoryWithinHDFS(String directoryPath) throws IOException {
        hdfs.mkdirs(new Path(directoryPath));
    }
        
    private static void deleteDirectoryWithinHDFS(String directoryPath) throws IOException {
        hdfs.delete(new Path(directoryPath), true);
    }
        
    private static void copyDirectoryFilesWithinHDFS(String sourceDirectory, String destinationDirectory, Configuration conf) throws IOException {
        RemoteIterator<LocatedFileStatus> sourceFiles = hdfs.listFiles(new Path(sourceDirectory), true);
        Path destinationPath = new Path(destinationDirectory);
        
        if (sourceFiles != null) {
            while(sourceFiles.hasNext()) {
                FileUtil.copy(hdfs, sourceFiles.next().getPath(), hdfs, destinationPath, true, conf);
            }           
        }
    }
    
    private static void cleanWorkspace(Configuration conf) throws IOException {
        deleteDirectoryWithinHDFS(conf.get("sampledMeans"));
        deleteDirectoryWithinHDFS(conf.get("intermediateMeans"));
        deleteDirectoryWithinHDFS(conf.get("clusteringClosestPoints"));
        deleteDirectoryWithinHDFS(conf.get("clusteringFinalMeans"));
        deleteDirectoryWithinHDFS(conf.get("convergence"));
        
        // Create new iteration means directory.
        createDirectoryWithinHDFS(conf.get("intermediateMeans"));
    }

    private static double parseObjectiveFunction(Configuration conf) throws IOException {
        double objectiveFunction = Double.POSITIVE_INFINITY;
        
        // Single value inside a single file. Guaranteed by the single reducer for convergence.
        FSDataInputStream hdfsDataInputStream = hdfs.open(new Path(conf.get("convergence") + "/part-r-00000"));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfsDataInputStream));
        String line = "";

        // It is a one line only file.
        while ((line = bufferedReader.readLine()) != null) {
            objectiveFunction = Double.parseDouble(line);
        }
        
        bufferedReader.close();
        
        return objectiveFunction;
    }
    
    private static void executeSampling(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
        Job sampling = Job.getInstance(conf, "sampling");
        
        if (!Sampling.main(sampling)) {
           System.err.println("****** ERROR: the sampling of the initial means failed. Exiting the job. ******\n");
           hdfs.close();
           System.exit(1);
        }
        
        System.out.println("****** SUCCESS: the sampling of the initial means succeeded. ******\n");
    }
    
    private static void executeKMeansIteration(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
        Job clusteringClosestPoints = Job.getInstance(conf, "clustering_closest_points"); 
        if (!Clustering_ClosestPoints.main(clusteringClosestPoints)) {
            System.err.println("****** ERROR: the clustering (closest points phase) iteration failed. Exiting the job. ******\n");
            hdfs.close();
            System.exit(1);
        }   
        System.out.println("****** SUCCESS: the clustering (closest points phase) iteration succeeded. ******\n");
            
            
        Job clusteringFinalMeans = Job.getInstance(conf, "clustering_final_means");
        if (!Clustering_FinalMeans.main(clusteringFinalMeans)) {
            System.err.println("****** ERROR: the clustering (final means phase) iteration failed. Exiting the job. ******\n");
            hdfs.close();
            System.exit(1);
        }
        System.out.println("****** SUCCESS: the clustering (final means phase) iteration succeeded. ******\n");

          
        Job convergence = Job.getInstance(conf, "convergence");
        if (!Convergence.main(convergence)) {
            System.err.println("****** ERROR: the convergence iteration failed. Exiting the job. ******\n");
            hdfs.close();
            System.exit(1);
        }
        System.out.println("****** SUCCESS: the convergence iteration succeeded. ******\n");
    }
    
    private static boolean stopConditionMet(Configuration conf, int iterationNumber, double objectiveFunction, double lastObjectiveFunction) throws IOException {
        System.out.println("****** Iteration number: " + (iterationNumber + 1) + " ******");
        System.out.println("****** Last objective function value: " + lastObjectiveFunction + " ******");
        System.out.println("****** Current objective function value: " + objectiveFunction + " ******\n");
        
        if (iterationNumber == 0) {
            System.out.println("****** First iteration: stop condition not checked. ******\n");
            return false;
        }
        
        double error = 100*((lastObjectiveFunction - objectiveFunction)/lastObjectiveFunction);
        double errorThreshold = conf.getDouble("errorThreshold", 1);
        
        System.out.println("****** Current error: " + error + "% ******");
        System.out.println("****** Error threshold: " + errorThreshold + "% ******\n");
        
        if (error <= errorThreshold) {
            System.out.println("****** Stop condition met: error " + error + "% ******\n");
            return true;
        }
        
        return false;
    }
    
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        LocalConfiguration localConfig = new LocalConfiguration("config.ini");
        localConfig.printConfiguration();
        
        Configuration conf = new Configuration();
        setupConfiguration(localConfig, conf);
        
        try {
            hdfs = FileSystem.get(conf);
            cleanWorkspace(conf);

            // First step: select the initial random means.
            executeSampling(conf);
            copyDirectoryFilesWithinHDFS(conf.get("sampledMeans"), conf.get("intermediateMeans"), conf);

            // Second step: update the means until a stop condition is met.
            int completedIterations = 0;
            double lastObjectiveFunction = Double.POSITIVE_INFINITY;

            while (completedIterations < localConfig.getMaximumNumberOfIterations()) {
                executeKMeansIteration(conf);
                double objectiveFunction = parseObjectiveFunction(conf);

                if (stopConditionMet(conf, completedIterations, objectiveFunction, lastObjectiveFunction)) {
                    hdfs.close();
                    return;
                }
                
                lastObjectiveFunction = objectiveFunction;

                deleteDirectoryWithinHDFS(conf.get("intermediateMeans"));
                createDirectoryWithinHDFS(conf.get("intermediateMeans"));
                copyDirectoryFilesWithinHDFS(conf.get("clusteringFinalMeans"), conf.get("intermediateMeans"), conf);

                deleteDirectoryWithinHDFS(conf.get("clusteringClosestPoints"));
                deleteDirectoryWithinHDFS(conf.get("clusteringFinalMeans"));
                deleteDirectoryWithinHDFS(conf.get("convergence"));

                completedIterations++;
            }
            
            System.out.println("****** Maximum number of iterations reached: " + completedIterations + " ******");
        } catch (Exception e) {
            System.err.println(e.getStackTrace());
        } finally {
            if (hdfs != null)
                hdfs.close();
        }
    }
}