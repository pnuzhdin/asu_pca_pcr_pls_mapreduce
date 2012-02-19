/** Altai State University, 2011 */
package edu.asu.hadoop.pcr.leastsquares.parameter;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.asu.hadoop.ConfigurationUtils;
import edu.asu.hadoop.DoubleArrayWritable;
import edu.asu.hadoop.pca.transformation.PCATransformation;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;


/**
 * Calculate regression parameter (PCR, MLP, least squares) for Hadoop 0.21.0 (Mapper)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CalculateRegressionParameterMapper
    extends Mapper<VLongWritable, DoubleArrayWritable, VIntWritable, DoubleArrayWritable> {
    
    private int dataDimFrom;
    private int dataDimTo;
    private long samplesCount;
    private int universeSize;
    private int dependentVariable;
    private double[] meanValues;
    private double[] stdevValues;
    private double[] eigenValues;
    private double[][] PCAComponents;
    private int universePCASz;
    private Boolean hyperspectralIsIgnoreOutliers;
    private double[] hyperspectralClasses;

    // hint
    private VIntWritable mapOutKey = new VIntWritable(1);
    private DoubleWritable[] outArray;
    private DoubleArrayWritable mapOutArray = new DoubleArrayWritable();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        samplesCount = conf.getLong("samplesCount", 0);
        universeSize = dataDimTo - dataDimFrom + 1;
        dependentVariable = conf.getInt("dependentVariable", 0);
        meanValues = ConfigurationUtils.getVector(conf, "meanValues");
        stdevValues = ConfigurationUtils.getVector(conf, "stdevValues");
        eigenValues = ConfigurationUtils.getVector(conf, "eigenValues");
        PCAComponents = ConfigurationUtils.getMatrix(conf, "PCAComponents");
        hyperspectralIsIgnoreOutliers = conf.getBoolean("hyperspectralIsIgnoreOutliers", false);
        hyperspectralClasses = ConfigurationUtils.getVector(conf, "hyperspectralClasses");
        universePCASz = PCAComponents.length;
        outArray = new DoubleWritable[universePCASz + 1];
        for(int i = 0; i <= universePCASz; i++) {
            outArray[i] = new DoubleWritable();
        }
    }

    @Override
    public void map(
            VLongWritable key,
            DoubleArrayWritable array,
            Context context) throws IOException, InterruptedException {
        checkNotConsidered: if (hyperspectralClasses != null) {
            int val = (int) array.get(dependentVariable).get();
            for (double cls : hyperspectralClasses) {
                if ((int) val == (int) cls) {
                    break checkNotConsidered;
                }
            }
            return;
        }
        checkOutliers: if (hyperspectralIsIgnoreOutliers) {
            double lastValue = array.get(0).get();
            for (int i = dataDimFrom; i < dataDimTo + 1; i++) {
                if (lastValue != array.get(i).get()) {
                    break checkOutliers;
                }
            }
            return;
        }
        double[] inVector = PCATransformation.vectorToPCA(
                array.getVector(dataDimFrom, dataDimTo),
                PCAComponents,
                meanValues,
                stdevValues);
        double dependentVal = array.get(dependentVariable).get();
        for (int i = 0; i < universePCASz; i++) {
            double outVal = (inVector[i] / eigenValues[i]) * (dependentVal / samplesCount);
            outArray[i].set(outVal);
        }
        outArray[universePCASz].set(dependentVal / samplesCount); // intercept
        mapOutArray.set(outArray);
        context.write(mapOutKey, mapOutArray);
    }
    
}
