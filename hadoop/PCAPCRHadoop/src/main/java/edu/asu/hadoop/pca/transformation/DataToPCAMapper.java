/** Altai State University, 2011 */
package edu.asu.hadoop.pca.transformation;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.asu.hadoop.ConfigurationUtils;
import edu.asu.hadoop.DoubleArrayWritable;
import org.apache.hadoop.io.VLongWritable;


/**
 * Matrix multiplication for Hadoop 0.21.0 (Mapper)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class DataToPCAMapper
    extends Mapper<VLongWritable, DoubleArrayWritable, VLongWritable, DoubleArrayWritable> {
    
    //NOTE: It's not used anymore
    
    private int dataDimFrom;
    private int dataDimTo;
    private int dataDimSize;
    private int universeSize;
    private double[] meanValues;
    private double[] stdevValues;
    private double[][] PCAComponents;
    
    // hint
    private DoubleWritable[] outArray;
    
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        dataDimSize = conf.getInt("dataDimSize", 0);
        universeSize = dataDimTo - dataDimFrom + 1;
        meanValues = ConfigurationUtils.getVector(conf, "meanValues");
        stdevValues = ConfigurationUtils.getVector(conf, "stdevValues");
        PCAComponents = ConfigurationUtils.getMatrix(conf, "PCAComponents");
//        int 
//        outArray = new DoubleWritable[dataDimSize - (universeSize - PCAComponents.length)];
//        for (int i = 0; i < )
    }
    
    @Override
    public void map(
            VLongWritable key,
            DoubleArrayWritable array,
            Context context) throws IOException, InterruptedException {
//        for (int p = 0; p < PCAComponents.length; p++) {
//            outArray[p + dataDimFrom] = new DoubleWritable(0);
//            for (int i = 0; i < universeSize; i++) {
//                double pointVal;
//                if (meanValues != null) {
//                    pointVal = array.get(i+dataDimFrom).get() - meanValues[i];
//                    if (stdevValues != null) {
//                        pointVal /= stdevValues[i];
//                    }
//                } else {
//                    pointVal = array.get(i+dataDimFrom).get();
//                }
//                outArray[p + dataDimFrom].set(outArray[p + dataDimFrom].get() + pointVal * PCAComponents[p][i]);
//            }
//        }
//        for (int i = 0; i < dataDimFrom; i++) {
//            outArray[i] = array.get(i);
//        }
//        for (int i = dataDimFrom + PCAComponents.length; i < outArray.length; i++) {
//            outArray[i] = array.get(i+(universeSize - PCAComponents.length));
//        }
//        context.write(key, new DoubleArrayWritable(outArray));
    }
    
}
