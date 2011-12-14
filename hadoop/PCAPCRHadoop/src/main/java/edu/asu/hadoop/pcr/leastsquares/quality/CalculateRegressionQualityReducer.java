/** Altai State University, 2011 */
package edu.asu.hadoop.pcr.leastsquares.quality;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Calculate regression quality parameters for Hadoop 0.21.0 (Reducer)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CalculateRegressionQualityReducer
    extends Reducer<VIntWritable, DoubleWritable, VIntWritable, DoubleWritable> {
    
    private DoubleWritable outVal = new DoubleWritable();
    
    @Override
    public void reduce(
            VIntWritable errorType,
            Iterable<DoubleWritable> errors,
            Context context) throws IOException, InterruptedException {
        double errorSum = 0;
        for (DoubleWritable error : errors) {
            errorSum += error.get();
        }
        if (errorType.get() == 1) {
            errorSum = Math.sqrt(errorSum);
        }
        outVal.set(errorSum);
        context.write(errorType, outVal);
    }
    
}
