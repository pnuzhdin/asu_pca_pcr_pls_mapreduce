/** Altai State University, 2011 */
package edu.asu.hadoop.pcapcr;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.TreeMap;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.math.linear.EigenDecompositionImpl;
import org.apache.commons.math.linear.Array2DRowRealMatrix;
import org.apache.commons.math.linear.RealMatrix;

import edu.asu.hadoop.covariance.CalculateCovarianceMapper;
import edu.asu.hadoop.covariance.CalculateCovarianceCombiner;
import edu.asu.hadoop.covariance.CalculateCovarianceReducer;
import edu.asu.hadoop.pcr.leastsquares.parameter.CalculateRegressionParameterMapper;
import edu.asu.hadoop.pcr.leastsquares.parameter.CalculateRegressionParameterReducer;
import edu.asu.hadoop.pcr.leastsquares.quality.CalculateRegressionQualityCombiner;
import edu.asu.hadoop.pcr.leastsquares.quality.CalculateRegressionQualityReducer;
import edu.asu.hadoop.ConfigurationUtils;
import edu.asu.hadoop.DoubleArrayWritable;
import edu.asu.hadoop.Point;
import edu.asu.hadoop.pcr.leastsquares.quality.CalculateRegressionQualityMapper;
import edu.asu.hadoop.preparedata.PrepareDataMapper;
import edu.asu.hadoop.scaledata.CalculateMeanAndSTDEVCombiner;
import edu.asu.hadoop.scaledata.CalculateMeanAndSTDEVMapper;
import edu.asu.hadoop.scaledata.CalculateMeanAndSTDEVReducer;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;


/**
 * PCA (covariance decomposition) or PCR (least squeares regression model) for Hadoop 0.21.0
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class PCAPCRHadoop
    extends Configured implements Tool {

    /**
     * Prepare data
     * 
     * @param conf Calculation configuration
     * @param inputDataPath Path to Data
     * @param outputDataPath Temp 
     * @return True in good case
     */
    public static boolean prepareData(
            Configuration conf,
            Path inputDataPath,
            Path outputDataPath) throws InterruptedException, ClassNotFoundException, IOException {
        Job job = new Job(conf, "edu.asu.hadoop.covariancedecomposition.PCAPCRHadoop: prepareData");
        
        job.setJarByClass(PCAPCRHadoop.class);
        
        job.setMapperClass(PrepareDataMapper.class);
        job.setNumReduceTasks(2);
        //job.setSpeculativeExecution(false);
        
        job.setMapOutputKeyClass(VLongWritable.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);
        job.setOutputKeyClass(VLongWritable.class);
        job.setOutputValueClass(DoubleArrayWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.addInputPath(job, inputDataPath);
        FileOutputFormat.setOutputPath(job, outputDataPath);

        return job.waitForCompletion(true);
        //return true;
    }
    
    /**
     * Calculate vector of mean and stdev values for data
     * 
     * @param conf Calculation configuration
     * @param inputDataPath Path to Data
     * @param outputDataPath Temp 
     * @return Vector of mean values
     */
    public static boolean calculateMeanAndSTDEVVector(
            Configuration conf,
            int dataDimFrom,
            int dataDimTo,
            double[] mean,
            double[] stdev,
            Path inputDataPath,
            Path outputDataPath) throws InterruptedException, ClassNotFoundException, IOException {
        final int universeSize = dataDimTo - dataDimFrom + 1;
        
//        Job job = new Job(conf, "edu.asu.hadoop.covariancedecomposition.PCAPCRHadoop: calculateMeanVector");
//        
//        job.setJarByClass(PCAPCRHadoop.class);
//        
//        job.setMapperClass(CalculateMeanAndSTDEVMapper.class);
//        job.setCombinerClass(CalculateMeanAndSTDEVCombiner.class);
//        job.setReducerClass(CalculateMeanAndSTDEVReducer.class);
//        job.setNumReduceTasks(1);
//        //job.setSpeculativeExecution(false);
//        
//        job.setMapOutputKeyClass(VIntWritable.class);
//        job.setMapOutputValueClass(DoubleArrayWritable.class);
//        job.setOutputKeyClass(VIntWritable.class);
//        job.setOutputValueClass(DoubleArrayWritable.class);
//
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        
//        FileInputFormat.addInputPath(job, inputDataPath);
//        FileOutputFormat.setOutputPath(job, outputDataPath);
//
//        if (job.waitForCompletion(true)) {
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(outputDataPath);
            for (FileStatus status : fss) {
                Path path = status.getPath();
                if (!path.getName().startsWith("part-r")) {
                    continue;
                }
                SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
                VIntWritable key = new VIntWritable();
                DoubleArrayWritable value = new DoubleArrayWritable();
                reader.next(key, value);
                for (int i = 0; i < universeSize; i++) {
                    mean[i] = value.get(i).get();
                }
                for (int i = universeSize; i < universeSize*2; i++) {
                    stdev[i-universeSize] = value.get(i).get();
                }
                reader.close();
                return true;
            }
            return false;
//        } else {
//            return false;
//        }
    }
    
//    /**
//     * Calculate vector of mean values for data
//     * 
//     * @param conf Calculation configuration
//     * @param inputDataPath Path to Data
//     * @param outputDataPath Temp 
//     * @return Vector of mean values
//     */
//    public static double[] calculateMeanVector(
//            Configuration conf,
//            int dataDimFrom,
//            int dataDimTo,
//            Path inputDataPath,
//            Path outputDataPath) throws InterruptedException, ClassNotFoundException, IOException {
//        final int universeSize = dataDimTo - dataDimFrom + 1;
//        
//        Job job = new Job(conf, "edu.asu.hadoop.covariancedecomposition.PCAPCRHadoop: calculateMeanVector");
//        
//        job.setJarByClass(PCAPCRHadoop.class);
//        
//        job.setMapperClass(CalculateCenterMapper.class);
//        job.setCombinerClass(CalculateCenterReducer.class);
//        job.setReducerClass(CalculateCenterReducer.class);
//        //job.setNumReduceTasks(1);
//        
//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(DoubleArrayWritable.class);
//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputValueClass(DoubleArrayWritable.class);
//
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        
//        FileInputFormat.addInputPath(job, inputDataPath);
//        FileOutputFormat.setOutputPath(job, outputDataPath);
//
//        if (job.waitForCompletion(true)) {
//            double[] meansVector = new double[universeSize];
//            
//            FileSystem fs = FileSystem.get(conf);
//            FileStatus[] fss = fs.listStatus(outputDataPath);
//            for (FileStatus status : fss) {
//                Path path = status.getPath();
//                if (!path.getName().startsWith("part-r")) {
//                    continue;
//                }
//                SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
//                IntWritable key = new IntWritable();
//                DoubleArrayWritable value = new DoubleArrayWritable();
//                reader.next(key, value);
//                for (int i = 0; i < universeSize; i++) {
//                    meansVector[i] = value.get(i).get();
//                }
//                reader.close();
//                break;
//            }
//            return meansVector;
//        } else {
//            return null;
//        }
//    }
//    
//    /**
//     * Calculate vector of standart deviations for data
//     * 
//     * @param conf Calculation configuration
//     * @param meanVector Vector of mean values
//     * @param inputDataPath Path to Data
//     * @param outputDataPath Temp 
//     * @return Vector of standart deviations
//     */
//    public static double[] calculateSTDEVVector(
//            Configuration conf,
//            int dataDimFrom,
//            int dataDimTo,
//            Path inputDataPath,
//            Path outputDataPath) throws InterruptedException, ClassNotFoundException, IOException {
//        final int universeSize = dataDimTo - dataDimFrom + 1;
//        
//        Job job = new Job(conf, "edu.asu.hadoop.covariancedecomposition.PCAPCRHadoop: calculateSTDEVVector");
//        
//        job.setJarByClass(PCAPCRHadoop.class);
//        
//        job.setMapperClass(CalculateSTDEVMapper.class);
//        job.setCombinerClass(CalculateSTDEVCombiner.class);
//        job.setReducerClass(CalculateSTDEVReducer.class);
//        //job.setNumReduceTasks(2);
//        
//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(DoubleArrayWritable.class);
//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputValueClass(DoubleArrayWritable.class);
//
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        
//        FileInputFormat.addInputPath(job, inputDataPath);
//        FileOutputFormat.setOutputPath(job, outputDataPath);
//
//        if (job.waitForCompletion(true)) {
//            double[] stdevVector = new double[dataDimTo - dataDimFrom + 1];
//            FileSystem fs = FileSystem.get(conf);
//            FileStatus[] fss = fs.listStatus(outputDataPath);
//            for (FileStatus status : fss) {
//                Path path = status.getPath();
//                if (!path.getName().startsWith("part-r")) {
//                    continue;
//                }
//                SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
//                IntWritable key = new IntWritable();
//                DoubleArrayWritable value = new DoubleArrayWritable();
//                reader.next(key, value);
//                for (int i = 0; i < universeSize; i++) {
//                    stdevVector[i] = value.get(i).get();
//                }
//                reader.close();
//            }
//            return stdevVector;
//        } else {
//            return null;
//        }
//    }
    
//    /**
//     * Scale data (centering & normalizing)
//     * 
//     * @param conf Calculation configuration
//     * @param meanVector Vector of mean values (may be null)
//     * @param stdevVector Vector of standart deviations (may be null) 
//     * @return True, if all ok, else false
//     */
//    public static boolean scaleData(
//            Configuration conf,
//            double[] meanVector,
//            double[] stdevVector,
//            Path inputDataPath,
//            Path outputDataPath) throws IOException, InterruptedException, ClassNotFoundException {
//        ConfigurationUtils.setVector(conf, "meanValues", meanVector);
//        ConfigurationUtils.setVector(conf, "stdevValues", stdevVector);
//        
//        Job job = new Job(conf, "edu.asu.hadoop.covariancedecomposition.PCAPCRHadoop: scaleData");
//        
//        job.setJarByClass(PCAPCRHadoop.class);
//        
//        job.setMapperClass(ScaleMapper.class);
//        
//        job.setMapOutputKeyClass(LongWritable.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(LongWritable.class);
//        job.setOutputValueClass(Text.class);
//
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        
//        FileInputFormat.addInputPath(job, inputDataPath);
//        FileOutputFormat.setOutputPath(job, outputDataPath);
//        
//        boolean result = job.waitForCompletion(true);
//        
//        return result;
//    }
    
    /**
     * Calculate Covariance matrix for data
     * 
     * @param conf Calculation configuration
     * @param meanVector Vector of mean values (may be null)
     * @param inputDataPath Path to Data that will be centering
     * @param outputDataPath Path where centered Data will be stored
     * @return Symmetric covariance matrix (compact form)
     */
    public static double[] calculateCovarianceMatrix(
            Configuration conf,
            int dataDimFrom,
            int dataDimTo,
            Path inputDataPath,
            Path outputDataPath) throws IOException, InterruptedException, ClassNotFoundException {
        int universeSize = dataDimTo - dataDimFrom + 1;
        int halfMatrixSize = (universeSize * (universeSize + 1)) / 2;
        
//        Job job = new Job(conf, "edu.asu.hadoop.covariancedecomposition.PCAPCRHadoop: calculateCovarianceMatrix");
//        
//        job.setJarByClass(PCAPCRHadoop.class);
//        
//        job.setMapperClass(CalculateCovarianceMapper.class);
//        job.setCombinerClass(CalculateCovarianceCombiner.class);
//        job.setReducerClass(CalculateCovarianceReducer.class);
//        job.setNumReduceTasks(1);
//        //job.setSpeculativeExecution(false);
//        
//        job.setMapOutputKeyClass(VIntWritable.class);
//        job.setMapOutputValueClass(DoubleArrayWritable.class);
//        job.setOutputKeyClass(VIntWritable.class);
//        job.setOutputValueClass(DoubleArrayWritable.class);
//
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        
//        FileInputFormat.addInputPath(job, inputDataPath);
//        FileOutputFormat.setOutputPath(job, outputDataPath);
//        
//        if (job.waitForCompletion(true)) {
            double[] covarianceMatrix = new double[halfMatrixSize];
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(outputDataPath);
            for (FileStatus status : fss) {
                Path path = status.getPath();
                if (!path.getName().startsWith("part-r")) {
                    continue;
                }
                SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
                VIntWritable key = new VIntWritable();
                DoubleArrayWritable value = new DoubleArrayWritable();
                reader.next(key, value);
                for (int i = 0; i < halfMatrixSize; i++) {
                    covarianceMatrix[i] = value.get(i).get();
                }
                reader.close();
                break;
            }
            return covarianceMatrix;
//        } else {
//            return null;
//        }
    }
    
    /**
     * Calculate Eigen vectors and values (sorted by descending eigen values)
     * 
     * @param conf Calculation configuration
     * @param covarianceMatrix Symmetric covariance matrix (compact form)
     * @return Map, where key - eigen value, value - eigen vector (normal)
     */
    public static TreeMap<Double, double[]> calculateEigenDecomposition(
            Configuration conf,
            int dataDimFrom,
            int dataDimTo,
            double[] covarianceMatrix) {
        int universeSize = dataDimTo - dataDimFrom + 1;
        
        TreeMap<Double, double[]> eigen = new TreeMap<Double, double[]>();
        
        //Eigen decomposition by Apache Common Math:
        RealMatrix realMatrix = new Array2DRowRealMatrix(universeSize, universeSize);
        int covI = 0;
        for (int i = 0; i < universeSize; i++) { //Decompress symmetric compact matrix into Apache Common Math matrix
            for (int j = 0; j < universeSize; j++) {
                if (j < i) {
                    realMatrix.setEntry(i, j, realMatrix.getEntry(j, i));
                } else {
                    realMatrix.setEntry(i, j, covarianceMatrix[covI]);
                    covI++;
                }
            }
        }
        EigenDecompositionImpl decompositionImpl = new EigenDecompositionImpl(realMatrix, 0); //NOTE: Second parameter not used
        for (int i = 0; i < universeSize; i++) {
            double eigenVectorNorm = decompositionImpl.getEigenvector(i).getNorm();
            eigen.put(decompositionImpl.getRealEigenvalue(i), decompositionImpl.getEigenvector(i).mapDivide(eigenVectorNorm).getData());
        }
        
        return eigen;
    }
    
    /**
     * Kaiser's rule to select number of components 
     * 
     * @param conf Calculation configuration
     * @param eigen Eigen vectors and values
     * @param covarianceMatrix Symmetric covariance matrix (compact form)
     * @return Number of first eigenvectors (in descending sort order), which choosen as components
     */
    public static int componentsByKaisersRule(
            Configuration conf,
            int dataDimFrom,
            int dataDimTo,
            TreeMap<Double, double[]> eigen,
            double[] covarianceMatrix) {
        int universeSize = dataDimTo - dataDimFrom + 1;
        
        double trace = 0;
        for (int i = 0, j = 0; j < universeSize - 1; i += universeSize - j++) {
            trace += covarianceMatrix[i] / universeSize;
        }
        
        int components = 0;
        
        for (Double eigenValue : eigen.descendingKeySet()) {
            if (eigenValue <= trace) {
                break;
            }
            components++;
        }
        
        return components;
    }
    
    /**
     * Select number of components by given energy threshold
     * 
     * @param conf Calculation configuration
     * @param eigen Eigen vectors and values
     * @param covarianceMatrix Symmetric covariance matrix (compact form)
     * @param threshold  Energy threshold
     * @return Number of first eigenvectors, which choosen as components
     */
    public static int componentsByEnergyThreshold(
            Configuration conf,
            TreeMap<Double, double[]> eigen,
            double[] covarianceMatrix,
            double threshold) {
        double fullEnergy = 0;
        for (Double eigenValue : eigen.descendingKeySet()) {
            fullEnergy += eigenValue;
        }
        
        int components = 0;
        
        double energy = 0;
        for (Double eigenValue : eigen.descendingKeySet()) {
            energy += eigenValue;
            if ((energy / fullEnergy) >= threshold) {
                break;
            }
            components++;
        }
        
        return components;
    }
    
//    /**
//     * Transforming a data matrix into PCA space
//     * 
//     * @param conf Calculation configuration
//     * @param PCAcomponents Components
//     * @param meanVector Vector of mean values (may be null)
//     * @param stdevVector Vector of standart deviations (may be null)
//     * @param inputDataPath Training data set
//     * @param outputDataPath Output for map/reduce
//     * @return True, if all ok, else false
//     */
//    public static boolean dataToPCA(
//            Configuration conf,
//            Path inputDataPath,
//            Path outputDataPath) throws IOException, InterruptedException, ClassNotFoundException {
//        Job job = new Job(conf, "edu.asu.hadoop.covariancedecomposition.PCAPCRHadoop: dataToPCA");
//        
//        job.setJarByClass(PCAPCRHadoop.class);
//        
//        job.setMapperClass(DataToPCAMapper.class);
//        job.setNumReduceTasks(2);
//        
//        job.setMapOutputKeyClass(LongWritable.class);
//        job.setMapOutputValueClass(DoubleArrayWritable.class);
//        job.setOutputKeyClass(LongWritable.class);
//        job.setOutputValueClass(DoubleArrayWritable.class);
//
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        
//        FileInputFormat.addInputPath(job, inputDataPath);
//        FileOutputFormat.setOutputPath(job, outputDataPath);
//        
//        boolean result = job.waitForCompletion(true);
//        
//        return result;
//    }
    
    /**
     * Calculate regression parameter (MLR, least squares method)
     * 
     * @param conf Calculation configuration
     * @param eigenValues Eigen values - components of product T'T , where T - score matrix
     * @param inputDataPath Training data set
     * @param outputDataPath Output for map/reduce
     * @return Regression parameter
     */
    public static double[] calculateRegressionParameter(
            Configuration conf,
            int parameterSize,
            Path inputDataPath,
            Path outputDataPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf, "edu.asu.hadoop.covariancedecomposition.PCAPCRHadoop: calculateRegressionParameter");
        
        job.setJarByClass(PCAPCRHadoop.class);
        
        job.setMapperClass(CalculateRegressionParameterMapper.class);
        job.setCombinerClass(CalculateRegressionParameterReducer.class);
        job.setReducerClass(CalculateRegressionParameterReducer.class);
        job.setNumReduceTasks(1);
        //job.setSpeculativeExecution(false);
        
        job.setMapOutputKeyClass(VIntWritable.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);
        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(DoubleArrayWritable.class);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.addInputPath(job, inputDataPath);
        FileOutputFormat.setOutputPath(job, outputDataPath);
        
        if (job.waitForCompletion(true)) {
            double[] regressionParameter = new double[parameterSize];
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(outputDataPath);
            for (FileStatus status : fss) {
                Path path = status.getPath();
                if (!path.getName().startsWith("part-r")) {
                    continue;
                }
                SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
                VIntWritable key = new VIntWritable();
                DoubleArrayWritable value = new DoubleArrayWritable();
                reader.next(key, value);
                for (int i = 0; i < parameterSize; i++) {
                    regressionParameter[i] = value.get(i).get();
                }
                reader.close();
                break;
            }
            return regressionParameter;
        } else {
            return null;
        }
    }
    
//    /**
//     * Calculate regression errors
//     * 
//     * @param conf Calculation configuration
//     * @param regressionParameter Regression parameter
//     * @param inputDataPath Data set
//     * @param outputDataPath Output for map/reduce
//     * @return True in good case
//     */
//    public static boolean calculateRegressionErrors(
//            Configuration conf,
//            Path inputDataPath,
//            Path outputDataPath) throws IOException, InterruptedException, ClassNotFoundException {
//        Job job = new Job(conf, "edu.asu.hadoop.covariancedecomposition.PCAPCRHadoop: calculateRegressionErrors");
//        
//        job.setJarByClass(PCAPCRHadoop.class);
//        
//        job.setMapperClass(CalculateRegressionErrorsMapper.class);
//        job.setNumReduceTasks(2);
//        
//        job.setMapOutputKeyClass(LongWritable.class);
//        job.setMapOutputValueClass(DoubleWritable.class);
//        job.setOutputKeyClass(LongWritable.class);
//        job.setOutputValueClass(DoubleWritable.class);
//        
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        
//        FileInputFormat.addInputPath(job, inputDataPath);
//        FileOutputFormat.setOutputPath(job, outputDataPath);
//        
//        return job.waitForCompletion(true);
//    }
    
    /**
     * Calculate regression quality (Mean Absolute Error, Root Mean Squared Error)
     * 
     * @param conf Calculation configuration
     * @param regressionParameter Regression parameter
     * @param inputDataPath Errors path
     * @param outputDataPath Output for map/reduce
     * @return Vector of modle quality parameters: {MAE, RMSE}
     */
    public static double[] calculateRegressionQuality(
            Configuration conf,
            Path inputDataPath,
            Path outputDataPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf, "edu.asu.hadoop.covariancedecomposition.PCAPCRHadoop: calculateRegressionQuality");
        
        job.setJarByClass(PCAPCRHadoop.class);
        
        job.setMapperClass(CalculateRegressionQualityMapper.class);
        job.setCombinerClass(CalculateRegressionQualityCombiner.class);
        job.setReducerClass(CalculateRegressionQualityReducer.class);
        job.setNumReduceTasks(2);
        //job.setSpeculativeExecution(false);
        
        job.setMapOutputKeyClass(VIntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.addInputPath(job, inputDataPath);
        FileOutputFormat.setOutputPath(job, outputDataPath);
        
        if (job.waitForCompletion(true)) {
            double[] errors = new double[2];
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(outputDataPath);
            for (FileStatus status : fss) {
                Path path = status.getPath();
                if (!path.getName().startsWith("part-r")) {
                    continue;
                }
                SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
                VIntWritable key = new VIntWritable();
                DoubleWritable value = new DoubleWritable();
                while (reader.next(key, value)) {
                    errors[key.get()] = value.get();
                }
                reader.close();
            }
            return errors;
        } else {
            return null;
        }
    }
    
    enum ComponentSelectBy {
        KAISER, THRESHOLD;
        
        @Override
        public String toString() {
            String s = super.toString();
            return s.substring(0, 1) + s.substring(1).toLowerCase();
        }
        
        public static ComponentSelectBy fromString(String str) {
            if ("Kaiser".equals(str)) {
                return KAISER;
            } else if (str.matches("Threshold-0.[0-9]+")) {
                return THRESHOLD;
            } else {
                return null;
            }
        }        
    }
    
    public static void writeVector(Configuration conf, Path path, double[] vector) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, IntWritable.class, Text.class);
        Point point = new Point(0, vector.length - 1, vector.length, Double.valueOf(0));
        for (int i = 0; i < vector.length; i++) {
            point.set(i, vector[i]);
        }
        writer.append(new IntWritable(0), point.toText());
        writer.close();
    }
    
    public static void writeMatrix(Configuration conf, Path path, double[][] matrix) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, IntWritable.class, Text.class);
        for (int i = 0; i < matrix.length; i++) {
            Point point = new Point(0, matrix[i].length - 1, matrix[i].length, Double.valueOf(0));
            for (int j = 0; j < matrix[i].length; j++) {
                point.set(i, matrix[i][j]);
            }
            writer.append(new IntWritable(i), point.toText());
        }
        writer.close();
    }
    
    public static void printTime(OutputStream os, String actionName, long elapsedTime) throws IOException {
        String logStr = "Action [" + actionName + "] elapsed time: " + String.valueOf(elapsedTime) + "\r\n";
        os.write(logStr.getBytes());
    }
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 14) {
            System.err.printf("Usage: program [generic options]\n"
                    + "{Data dimension size}\n"
                    + "{Data dimension from}\n"
                    + "{Data dimension to}\n"
                    + "{Dependent variable position}\n"
                    + "{Training set size}\n"
                    + "{Testing set size}\n"
                    + "{Is need data centring: y | n}\n"
                    + "{Is need data normalizing: y | n}\n"
                    + "{Select components by: Kaiser | Threshold-[VALUE]}\n"
                    + "{Training set path}\n"
                    + "{Testing set path}\n"
                    + "{Temp data path}\n"
                    + "{Elapsed time path}\n");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        
        long curTime = System.currentTimeMillis();
        long taskBeginTime = curTime;
        
        final int dataSize = Integer.valueOf(args[0]);
        final int dataDimFrom = Integer.valueOf(args[1]);
        final int dataDimTo = Integer.valueOf(args[2]);
        final int dependentVariable = Integer.valueOf(args[3]);
        final long trainingSetSize = Long.valueOf(args[4]);
        final long testingSetSize = Long.valueOf(args[5]);
        final boolean isNeedDataCentring = "y".equals(args[6]);
        final boolean isNeedDataNormalizing = "y".equals(args[7]);
        final ComponentSelectBy componentSelectBy =
                ComponentSelectBy.fromString(args[8]);
        double thresholdValue = 0.9;
        if (componentSelectBy == ComponentSelectBy.THRESHOLD) {
            thresholdValue = Double.valueOf(args[8].split("-")[1]);
        }
        final Path trainingSetPath = new Path(args[9]);
        final Path testingSetPath = new Path(args[10]);
        //final Path outputDataPath = new Path(args[11]);
        final String tempDataPath = args[12];
        final String elapsedTimePath = args[13];
        
        FileOutputStream elapsedTimeStream = new FileOutputStream(elapsedTimePath, true);
        elapsedTimeStream.write("-----------------\r\n".getBytes());
        
        Configuration conf = getConf();
        
        FileSystem fs = FileSystem.get(conf);
        
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.setInt("dataDimFrom", dataDimFrom);
        conf.setInt("dataDimTo", dataDimTo);
        conf.setLong("samplesCount", trainingSetSize);
        conf.setInt("dataDimSize", dataSize);
        conf.setInt("dependentVariable", dependentVariable);
        
        int universeSize = dataDimTo - dataDimFrom + 1;
        double[] meanVector = new double[universeSize];
        double[] stdevVector = new double[universeSize];
        
        curTime = System.currentTimeMillis();
        Path trainingSetPreparedPath = new Path(tempDataPath + "-trainingsetprepared");
//        prepareData(conf, trainingSetPath, trainingSetPreparedPath);
        printTime(elapsedTimeStream, "prepareTrainingSet", System.currentTimeMillis() - curTime);
        
        if (isNeedDataCentring) {
            curTime = System.currentTimeMillis();
            Path meanDataPath = new Path(tempDataPath + "-meanandstdev");
            calculateMeanAndSTDEVVector(conf, dataDimFrom, dataDimTo, meanVector, stdevVector, trainingSetPreparedPath, meanDataPath);
            printTime(elapsedTimeStream, "calculateMeanAndSTDEVVector", System.currentTimeMillis() - curTime);
            
            ConfigurationUtils.setVector(conf, "meanValues", meanVector);
            if (isNeedDataNormalizing) {
                ConfigurationUtils.setVector(conf, "stdevValues", stdevVector);
            }
        }
        
//        Path trainingSetScaledPath = new Path(tempDataPath + "-trainingsetscaled");
//        if (isNeedDataCentring && isNeedDataNormalizing) {
//            curTime = System.currentTimeMillis();
//            scaleData(conf, meanVector, stdevVector, trainingSetPath, trainingSetScaledPath);
//            printTime(elapsedTimeStream, "scaleTrainingSet", System.currentTimeMillis() - curTime);
//        }
        
        // PCA
        
        curTime = System.currentTimeMillis();
        Path covarianceDataPath = new Path(tempDataPath + "-covariance");
        double[] covarianceMatrix = calculateCovarianceMatrix(conf, dataDimFrom, dataDimTo, trainingSetPreparedPath, covarianceDataPath);
        printTime(elapsedTimeStream, "calculateCovarianceMatrix", System.currentTimeMillis() - curTime);
        
        curTime = System.currentTimeMillis();
        TreeMap<Double, double[]> eigen = calculateEigenDecomposition(conf, dataDimFrom, dataDimTo, covarianceMatrix);
        printTime(elapsedTimeStream, "calculateEigenDecomposition", System.currentTimeMillis() - curTime);
        
        curTime = System.currentTimeMillis();
        int components = 0;
        if (componentSelectBy == ComponentSelectBy.KAISER) {
            components = componentsByKaisersRule(conf, dataDimFrom, dataDimTo, eigen, covarianceMatrix);
        } else if (componentSelectBy == ComponentSelectBy.THRESHOLD) {
            components = componentsByEnergyThreshold(conf, eigen, covarianceMatrix, thresholdValue);
        }
        printTime(elapsedTimeStream, "selectComponentsNumber (" + components + ")", System.currentTimeMillis() - curTime);
        
        curTime = System.currentTimeMillis();
        double[][] PCAComponents = new double[components][dataDimTo - dataDimFrom + 1];
        double[] eigenValues = new double[components];
        int i = 0;
        for (Double eigenValue : eigen.descendingMap().keySet()) {
            PCAComponents[i] = eigen.descendingMap().get(eigenValue);
            eigenValues[i] = eigenValue;
            if (++i >= components) {
                break;
            }
        }
        ConfigurationUtils.setMatrix(conf, "PCAComponents", PCAComponents);
        ConfigurationUtils.setVector(conf, "eigenValues", eigenValues);
        writeVector(conf, new Path(tempDataPath + "-eigenvalues"), eigenValues);
        writeMatrix(conf, new Path(tempDataPath + "-components"), PCAComponents);
//        Path PCATrainingSetPath = new Path(tempDataPath + "-trainingsetpca");
//        dataToPCA(conf, trainingSetPreparedPath, PCATrainingSetPath);
//        printTime(elapsedTimeStream, "trainingSetToPCA", System.currentTimeMillis() - curTime);
        
        printTime(elapsedTimeStream, "PCA", System.currentTimeMillis() - taskBeginTime);
        
        // Regression / Training
        
        curTime = System.currentTimeMillis();
        Path regressionParameterPath = new Path(tempDataPath + "-regressionparameter");
        double[] regressionParameter = calculateRegressionParameter(conf, components + 1,trainingSetPreparedPath, regressionParameterPath);
        ConfigurationUtils.setVector(conf, "regressionParameter", regressionParameter);
        writeVector(conf, new Path(tempDataPath + "-regressionparametertext"), regressionParameter);
        printTime(elapsedTimeStream, "calculateRegressionParameter", System.currentTimeMillis() - curTime);
        
//        curTime = System.currentTimeMillis();
//        Path trainingSetErrorsPath = new Path(tempDataPath + "-trainingseterrors");
//        calculateRegressionErrors(conf, PCATrainingSetPath, trainingSetErrorsPath);
//        printTime(elapsedTimeStream, "calculatingTrainingErrors", System.currentTimeMillis() - curTime);

        curTime = System.currentTimeMillis();
        Path trainingSetQualityPath = new Path(tempDataPath + "-trainingsetquality");
        double[] trainingQuality = calculateRegressionQuality(conf, trainingSetPreparedPath, trainingSetQualityPath);
        printTime(elapsedTimeStream, "calculatingTrainingQuality (MAE: " + trainingQuality[0] + ", RMSE: " + trainingQuality[1] + ")", System.currentTimeMillis() - curTime);
        
        // Regression / Testing
        
        conf.setLong("samplesCount", testingSetSize);
        
        curTime = System.currentTimeMillis();
        Path testingSetPreparedPath = new Path(tempDataPath + "-testingsetprepared");
        prepareData(conf, testingSetPath, testingSetPreparedPath);
        printTime(elapsedTimeStream, "prepareTestingSet", System.currentTimeMillis() - curTime);
        
//        curTime = System.currentTimeMillis();
//        Path PCATestingSetPath = new Path(tempDataPath + "-testingsetpca");
//        dataToPCA(conf, testingSetPreparedPath, PCATestingSetPath);
//        printTime(elapsedTimeStream, "testingSetToPCA", System.currentTimeMillis() - curTime);
        
//        curTime = System.currentTimeMillis();
//        Path testingSetErrorsPath = new Path(tempDataPath + "-testingseterrors");
//        calculateRegressionErrors(conf, PCATestingSetPath, testingSetErrorsPath);
//        printTime(elapsedTimeStream, "calculatingTestingErrors", System.currentTimeMillis() - curTime);
        
        curTime = System.currentTimeMillis();
        Path testingSetQualityPath = new Path(tempDataPath + "-testingsetquality");
        double[] testingQuality = calculateRegressionQuality(conf, testingSetPreparedPath, testingSetQualityPath);
        printTime(elapsedTimeStream, "calculatingTestingQuality (MAE: " + testingQuality[0] + ", RMSE: " + testingQuality[1] + ")", System.currentTimeMillis() - curTime);
        
        printTime(elapsedTimeStream, "PCR", System.currentTimeMillis() - taskBeginTime);
        
        return 0;
    }
    
    public static void main( String[] args )
            throws Exception {
        System.exit(ToolRunner.run(new PCAPCRHadoop(), args));
    }
}
