/** Altai State University, 2011 */
package edu.asu.hadoop.pcr.leastsquares.parameter;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.asu.hadoop.DoubleArrayWritable;
import edu.asu.hadoop.ConfigurationUtils;


/**
 * Calculate regression parameter (PCR, MLP, least squares) for Hadoop 0.21.0 (Reducer & Combiner)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CalculateRegressionParameterReducer
    extends Reducer<VIntWritable, DoubleArrayWritable, VIntWritable, DoubleArrayWritable> {
    
    //TODO: Think about DRY

    private int dataDimFrom;
    private int dataDimTo;
    private int universeSize;
    private double[][] PCAComponents;
    private int universePCASz;

    // hint
    private DoubleWritable[] outArray;
    private DoubleArrayWritable reduceOutArray = new DoubleArrayWritable();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        universeSize = dataDimTo - dataDimFrom + 1;
        PCAComponents = ConfigurationUtils.getMatrix(conf, "PCAComponents");
        universePCASz = PCAComponents.length;
        outArray = new DoubleWritable[universePCASz + 1];
        for (int i = 0; i <= universePCASz; i++) {
            outArray[i] = new DoubleWritable();
        }
    }

    @Override
    public void reduce(
            VIntWritable column,
            Iterable<DoubleArrayWritable> partialSums,
            Context context) throws IOException, InterruptedException {
        boolean isFirst = true;
        for (DoubleArrayWritable partialSum : partialSums) {
            for (int i = 0; i <= universePCASz; i++) {
                double val = partialSum.get(i).get();
                if (!isFirst) {
                    val += outArray[i].get();
                }
                outArray[i].set(val);
            }
            if (isFirst) {
                isFirst = false;
            }
        }
        reduceOutArray.set(outArray);
        context.write(column, reduceOutArray);
    }
    
}
