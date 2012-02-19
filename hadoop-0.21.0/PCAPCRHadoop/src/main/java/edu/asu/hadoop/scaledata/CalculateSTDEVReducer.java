/** Altai State University, 2011 */
package edu.asu.hadoop.scaledata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.asu.hadoop.DoubleArrayWritable;


/**
 * Calculate STDEV for Hadoop 0.21.0 (Reducer)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CalculateSTDEVReducer
    extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {
    
    private int dataDimFrom;
    private int dataDimTo;
    private int universeSize;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        universeSize = dataDimTo - dataDimFrom + 1;
    }
    
    @Override
    public void reduce(
            IntWritable column,
            Iterable<DoubleArrayWritable> partialSums,
            Context context) throws IOException, InterruptedException {
        DoubleWritable[] outArray = new DoubleWritable[universeSize];
        boolean isFirst = true;
        for (DoubleArrayWritable partialSum : partialSums) {
            for (int i = 0; i < universeSize; i++) {
                if (!isFirst) {
                    outArray[i].set(outArray[i].get() + partialSum.get(i).get());
                } else {
                    outArray[i] = new DoubleWritable(partialSum.get(i).get());
                }
            }
            isFirst = false;
        }
        for (int i = 0; i < universeSize; i++) {
            outArray[i].set(Math.sqrt(outArray[i].get()));
        }
        context.write(column, new DoubleArrayWritable(outArray));
//        double val = 0;
//        for (DoubleWritable partialSum : partialSums) {
//            val += partialSum.get();
//        }
//        context.write(column, new DoubleWritable(Math.sqrt(val)));
    }
    
}
