/** Altai State University, 2011 */
package edu.asu.hadoop.covariance;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.asu.hadoop.DoubleArrayWritable;


/**
 * Calculate covariance matrix for Hadoop 0.21.0 (Reducer & Combiner)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CalculateCovarianceReducer
    extends Reducer<VIntWritable, DoubleArrayWritable, VIntWritable, DoubleArrayWritable> {

    private long samplesCount;
    private int dataDimFrom;
    private int dataDimTo;
    private int universeSize;
    private int outArraySize;

    // hint
    private DoubleWritable[] outArray;
    private DoubleArrayWritable reducerOutArray = new DoubleArrayWritable();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        samplesCount = conf.getLong("samplesCount", 0);
        universeSize = dataDimTo - dataDimFrom + 1;
        outArraySize = (universeSize * (universeSize + 1) / 2);
        outArray = new DoubleWritable[outArraySize];
        for (int i = 0; i < outArraySize; i++) {
            outArray[i] = new DoubleWritable();
        }
    }

    @Override
    public void reduce(
            VIntWritable position,
            Iterable<DoubleArrayWritable> compositions,
            Context context) throws IOException, InterruptedException {
        boolean isFirst = true;
        for (DoubleArrayWritable composition : compositions) {
            for (int i = 0; i < outArraySize; i++) {
                double val = composition.get(i).get();
                if (!isFirst) {
                    val += outArray[i].get();
                }
                outArray[i].set(val);
            }
            if (isFirst) {
                isFirst = false;
            }
        }
        reducerOutArray.set(outArray);
        context.write(position, reducerOutArray);
    }
    
}
