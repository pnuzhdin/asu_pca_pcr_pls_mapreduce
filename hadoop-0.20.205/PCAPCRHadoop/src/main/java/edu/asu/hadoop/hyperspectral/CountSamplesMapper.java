/** Altai State University, 2011 */
package edu.asu.hadoop.hyperspectral;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.asu.hadoop.ConfigurationUtils;
import edu.asu.hadoop.DoubleArrayWritable;


/**
 * Calculate samples count for Hadoop 0.20.205 (Mapper)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CountSamplesMapper
    extends Mapper<VLongWritable, DoubleArrayWritable, VIntWritable, VLongWritable> {
    
    private int dataDimFrom;
    private int dataDimTo;
    private int dependentVariable;
    private int universeSize;
    private Boolean hyperspectralIsIgnoreOutliers;
    private double[] hyperspectralClasses;
    
    // hint
    private VIntWritable mapOutKey = new VIntWritable(1);
    private VLongWritable mapOutValue = new VLongWritable(1);
    
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        dependentVariable = conf.getInt("dependentVariable", 0);
        hyperspectralIsIgnoreOutliers = conf.getBoolean("hyperspectralIsIgnoreOutliers", false);
        hyperspectralClasses = ConfigurationUtils.getVector(conf, "hyperspectralClasses");
        universeSize = dataDimTo - dataDimFrom + 1;
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
        context.write(mapOutKey, mapOutValue);
    }
    
}
