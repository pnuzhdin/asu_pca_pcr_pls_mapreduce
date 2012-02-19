/** Altai State University, 2011 */
package edu.asu.hadoop.pcr.leastsquares.quality;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.asu.hadoop.ConfigurationUtils;
import edu.asu.hadoop.DoubleArrayWritable;
import edu.asu.hadoop.pca.transformation.PCATransformation;


/**
 * Calculate regression quality parameters for Hadoop 0.21.0 (Mapper)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CalculateRegressionQualityMapper
    extends Mapper<VLongWritable, DoubleArrayWritable, VIntWritable, DoubleWritable> {
    
    private int dataDimFrom;
    private int dataDimTo;
    private int dependentVariable;
    private long samplesCount;
    private double[] meanValues;
    private double[] stdevValues;
    private double[] regressionParameter;
    private double[][] PCAComponents;
    private Boolean hyperspectralIsIgnoreOutliers;
    private double[] hyperspectralClasses;

    // hint
    private int universePCASz;
    private VIntWritable MAEKey = new VIntWritable(0);
    private VIntWritable RMSEKey = new VIntWritable(1);
    private DoubleWritable outVal = new DoubleWritable();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        dependentVariable = conf.getInt("dependentVariable", 0);
        samplesCount = conf.getLong("samplesCount", 0);
        meanValues = ConfigurationUtils.getVector(conf, "meanValues");
        stdevValues = ConfigurationUtils.getVector(conf, "stdevValues");
        regressionParameter = ConfigurationUtils.getVector(conf, "regressionParameter");
        PCAComponents = ConfigurationUtils.getMatrix(conf, "PCAComponents");
        hyperspectralIsIgnoreOutliers = conf.getBoolean("hyperspectralIsIgnoreOutliers", false);
        hyperspectralClasses = ConfigurationUtils.getVector(conf, "hyperspectralClasses");
        universePCASz = PCAComponents.length;
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
        double response = 0;
        for (int i = 0; i < universePCASz; i++) {
            response += inVector[i] * regressionParameter[i];
        }
        response += regressionParameter[universePCASz];
        double responseValue = response;
        if (hyperspectralClasses != null) {
            double lastDistance = Double.MAX_VALUE;
            for (double cls : hyperspectralClasses) {
                double curDistance = Math.abs(response - cls);
                if (curDistance < lastDistance) {
                    responseValue = cls;
                    lastDistance = curDistance;
                }
            }
        }
        double error = array.get(dependentVariable).get() - responseValue;
        outVal.set(Math.abs(error) / samplesCount);
        context.write(MAEKey, outVal); // MAE
        outVal.set((error / samplesCount) * error);
        context.write(RMSEKey, outVal); // RMSE
    }
    
}
