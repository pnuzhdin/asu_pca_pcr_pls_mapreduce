/** Altai State University, 2011 */
package edu.asu.hadoop.covariance;

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


/**
 * Calculate covariance matrix for Hadoop 0.20.205 (Mapper)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CalculateCovarianceMapper
    extends Mapper<VLongWritable, DoubleArrayWritable, VIntWritable, DoubleArrayWritable> {

    private long samplesCount;
    private int dataDimFrom;
    private int dataDimTo;
    private int dependentVariable;
    private int universeSize;
    private double[] meanValues;
    private double[] stdevValues;
    private int outArraySize;
    private Boolean hyperspectralIsIgnoreOutliers;
    private double[] hyperspectralClasses;
    
    // hint
    private VIntWritable mapOutKey = new VIntWritable(1);
    private DoubleArrayWritable mapOutArray = new DoubleArrayWritable();
    private DoubleWritable[] outArray;
    
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        dependentVariable = conf.getInt("dependentVariable", 0);
        meanValues = ConfigurationUtils.getVector(conf, "meanValues");
        stdevValues = ConfigurationUtils.getVector(conf, "stdevValues");
        samplesCount = conf.getLong("samplesCount", 0);
        hyperspectralIsIgnoreOutliers = conf.getBoolean("hyperspectralIsIgnoreOutliers", false);
        hyperspectralClasses = ConfigurationUtils.getVector(conf, "hyperspectralClasses");
        universeSize = dataDimTo - dataDimFrom + 1;
        outArraySize = (universeSize * (universeSize + 1) / 2);
        outArray = new DoubleWritable[outArraySize];
        for (int i = 0; i < outArraySize; i++) {
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
        int outKey = 0;
        for (int i = 0; i < universeSize; i++) {
            for (int j = i; j < universeSize; j++) {
                double outVal;
                if (meanValues != null) {
                    outVal = (array.get(i + dataDimFrom).get() - meanValues[i]) * (array.get(j + dataDimFrom).get() - meanValues[j]);
                    if (stdevValues != null) {
                        outVal /= stdevValues[i] * stdevValues[j];
                    }
                } else {
                    outVal = array.get(i + dataDimFrom).get() * array.get(j + dataDimFrom).get();
                }
                outArray[outKey++].set(outVal / samplesCount);
            }
        }
        mapOutArray.set(outArray);
        context.write(mapOutKey, mapOutArray);
    }
    
}
