/** Altai State University, 2011 */
package edu.asu.hadoop.scaledata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.asu.hadoop.DoubleArrayWritable;


/**
 * Centering Data for Hadoop 0.21.0 (Reducer & Combiner)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CalculateMeanAndSTDEVReducer
    extends Reducer<VIntWritable, DoubleArrayWritable, VIntWritable, DoubleArrayWritable> {
    
    private int dataDimFrom;
    private int dataDimTo;
    private int universeSize;
    
    // hint
    private DoubleWritable[] outArray;
    private DoubleArrayWritable reduceOutArray = new DoubleArrayWritable();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        universeSize = dataDimTo - dataDimFrom + 1;
        outArray = new DoubleWritable[universeSize*2];
        for (int i = 0; i < universeSize*2; i++) {
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
            for (int i = 0; i < universeSize*2; i++) {
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
        for (int i = universeSize; i < universeSize*2; i++) {
            double mean = outArray[i-universeSize].get();
            outArray[i].set(Math.sqrt(outArray[i].get() - mean*mean));
        }
        reduceOutArray.set(outArray);
        context.write(column, reduceOutArray);
    }
    
}
