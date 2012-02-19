/** Altai State University, 2012 */
package edu.asu.hadoop.pcapcr;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.TreeMap;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.asu.hadoop.ConfigurationUtils;
import edu.asu.hadoop.IntArrayWritable;
import edu.asu.hadoop.hyperspectral.HyperspectralBIPInputFormat;
import edu.asu.hadoop.hyperspectral.HyperspectralBIPOutputFormat;


/**
 * PCA (covariance decomposition) or PCR (least squeares regression model) for Hadoop 0.21.0 (BIP version)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class PCAPCRHadoop_BIP
    extends Configured implements Tool {

    public static boolean testBIP(
            Configuration conf,
            String dataFormat,
            int numBands,
            Path inputDataPath,
            Path outputDataPath) throws InterruptedException, ClassNotFoundException, IOException {
        Job job = new Job(conf, "edu.asu.hadoop.covariancedecomposition.PCAPCRHadoop: testBIP");
        
        job.setJarByClass(PCAPCRHadoop_BIP.class);
        
        job.setMapperClass(TrivialMapper.class);
        //job.setNumReduceTasks(2);
        //job.setSpeculativeExecution(false);
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntArrayWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntArrayWritable.class);

        HyperspectralBIPInputFormat.setDataFormat(job, dataFormat);
        HyperspectralBIPInputFormat.setNumBands(job, numBands);
        job.setInputFormatClass(HyperspectralBIPInputFormat.class);
        HyperspectralBIPOutputFormat.setDataFormat(job, dataFormat);
        HyperspectralBIPOutputFormat.setIsRoundDownward(job, true);
        job.setOutputFormatClass(HyperspectralBIPOutputFormat.class);

        FileInputFormat.addInputPath(job, inputDataPath);
        FileOutputFormat.setOutputPath(job, outputDataPath);

        return job.waitForCompletion(true);
        //return true;
    }
    
    public static void printTime(OutputStream os, String actionName, long elapsedTime) throws IOException {
        String logStr = "Action [" + actionName + "] elapsed time: " + String.valueOf(elapsedTime) + "\r\n";
        os.write(logStr.getBytes());
    }
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 10) {
            System.err.printf("Usage: program [generic options]\n"
                    + "{Image input format}\n"
                    + "{Image input bands}\n"
                    + "{Training set size}\n"
                    + "{Testing set size}\n"
                    + "{Is need data centring: y | n}\n"
                    + "{Is need data normalizing: y | n}\n"
                    + "{Training set path}\n"
                    + "{Testing set path}\n"
                    + "{Temp data path}\n"
                    + "{Elapsed time path}\n");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        
        long curTime = System.currentTimeMillis();
        long taskBeginTime = curTime;
        
        final String imageInputFormat = args[0];
        final int imageInputBands = Integer.valueOf(args[1]);
        final long trainingSetSize = Long.valueOf(args[2]);
        final long testingSetSize = Long.valueOf(args[3]);
        final boolean isNeedDataCentring = "y".equals(args[4]);
        final boolean isNeedDataNormalizing = "y".equals(args[5]);
        final Path trainingSetPath = new Path(args[6]);
        final Path testingSetPath = new Path(args[7]);
        //final Path outputDataPath = new Path(args[11]);
        final String tempDataPath = args[8];
        final String elapsedTimePath = args[9];
        
        FileOutputStream elapsedTimeStream = new FileOutputStream(elapsedTimePath, true);
        elapsedTimeStream.write("-----------------\r\n".getBytes());
        
        Configuration conf = getConf();
        
        FileSystem fs = FileSystem.get(conf);
        
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.set("imageInputFormat", imageInputFormat);
        conf.setInt("imageInputBands", imageInputBands);
        conf.setLong("trainingSetSize", trainingSetSize);
        conf.setLong("testingSetSize", testingSetSize);
        
        double[] meanVector = new double[imageInputBands];
        double[] stdevVector = new double[imageInputBands];

        curTime = System.currentTimeMillis();
        Path testBIPPath = new Path(tempDataPath + "-testbip");
        testBIP(conf, imageInputFormat, imageInputBands, trainingSetPath, testBIPPath);
        printTime(elapsedTimeStream, "testBIP", System.currentTimeMillis() - curTime);

//        curTime = System.currentTimeMillis();
//        Path trainingSetPreparedPath = new Path(tempDataPath + "-trainingsetprepared");
//        prepareData(conf, trainingSetPath, trainingSetPreparedPath);
//        printTime(elapsedTimeStream, "prepareTrainingSet", System.currentTimeMillis() - curTime);
        
//        if (isNeedDataCentring) {
//            curTime = System.currentTimeMillis();
//            Path meanDataPath = new Path(tempDataPath + "-meanandstdev");
//            calculateMeanAndSTDEVVector(conf, dataDimFrom, dataDimTo, meanVector, stdevVector, trainingSetPreparedPath, meanDataPath);
//            printTime(elapsedTimeStream, "calculateMeanAndSTDEVVector", System.currentTimeMillis() - curTime);
//
//            ConfigurationUtils.setVector(conf, "meanValues", meanVector);
//            if (isNeedDataNormalizing) {
//                ConfigurationUtils.setVector(conf, "stdevValues", stdevVector);
//            }
//        }
        
        printTime(elapsedTimeStream, "PCR", System.currentTimeMillis() - taskBeginTime);
        
        return 0;
    }
    
    public static void main( String[] args )
            throws Exception {
        System.exit(ToolRunner.run(new PCAPCRHadoop_BIP(), args));
    }
}
