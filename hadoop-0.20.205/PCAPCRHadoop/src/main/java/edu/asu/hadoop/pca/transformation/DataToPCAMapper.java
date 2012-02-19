/** Altai State University, 2011 */
package edu.asu.hadoop.pca.transformation;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.asu.hadoop.ConfigurationUtils;
import edu.asu.hadoop.DoubleArrayWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;


/**
 * Matrix multiplication for Hadoop 0.20.205 (Mapper)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class DataToPCAMapper
    extends Mapper<VLongWritable, DoubleArrayWritable, LongWritable, DoubleArrayWritable> {

    //NOTE: It's not used anymore

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
    private double[] regressionParameter;

    // hint
    private LongWritable mapOutKey = new LongWritable(1);
    private DoubleWritable[] outArray;
    private DoubleArrayWritable mapOutArray = new DoubleArrayWritable();

    @Override
    protected void setup(Mapper.Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        samplesCount = conf.getLong("samplesCount", 0);
        universeSize = dataDimTo - dataDimFrom + 1;
        dependentVariable = conf.getInt("dependentVariable", 0);
        meanValues = ConfigurationUtils.getVector(conf, "meanValues");
        stdevValues = ConfigurationUtils.getVector(conf, "stdevValues");
        eigenValues = ConfigurationUtils.getVector(conf, "eigenValues");
        regressionParameter = ConfigurationUtils.getVector(conf, "regressionParameter");
        PCAComponents = ConfigurationUtils.getMatrix(conf, "PCAComponents");
        universePCASz = PCAComponents.length;
        outArray = new DoubleWritable[universePCASz + 1];
        for (int i = 0; i <= universePCASz; i++) {
            outArray[i] = new DoubleWritable();
        }
        hyperspectralIsIgnoreOutliers = conf.getBoolean("hyperspectralIsIgnoreOutliers", false);
        hyperspectralClasses = ConfigurationUtils.getVector(conf, "hyperspectralClasses");
    }

    @Override
    public void map(
            VLongWritable key,
            DoubleArrayWritable array,
            Context context) throws IOException, InterruptedException {
        double[] inVector = PCATransformation.vectorToPCA(
                array.getVector(dataDimFrom, dataDimTo),
                PCAComponents,
                meanValues,
                stdevValues);
        //ACHTUNG!!!
//        for (int i = 0; i < dataDimFrom; i++) {
//            outArray[i].set(array.get(i).get());
//        }
        for (int i = 0; i < universePCASz; i++) {
            outArray[i].set(inVector[i]);
        }
//        for (int i = dataDimFrom + universePCASz; i < outArray.length; i++) {
//            outArray[i].set(array.get(i+(universeSize - universePCASz)).get());
//        }
        checkOutliers: if (hyperspectralIsIgnoreOutliers) {
            double lastValue = array.get(0).get();
            for (int i = dataDimFrom; i < dataDimTo + 1; i++) {
                if (lastValue != array.get(i).get()) {
                    break checkOutliers;
                }
            }
            outArray[universePCASz].set(array.get(dependentVariable).get());
            mapOutKey.set(key.get());
            mapOutArray.set(outArray);
            context.write(mapOutKey, mapOutArray);
            return;
        }
        double response = 0;
        for (int k = 0; k < universePCASz; k++) {
            response += inVector[k] * regressionParameter[k];
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
        outArray[universePCASz].set(responseValue);
        mapOutKey.set(key.get());
        mapOutArray.set(outArray);
        context.write(mapOutKey, mapOutArray);
    }

}