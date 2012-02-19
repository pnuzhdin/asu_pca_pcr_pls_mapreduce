/** Altai State University, 2011 */
package edu.asu.hadoop.hyperspectral;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Calculate samples count for Hadoop 0.21.0 (Reducer & Combiner)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CountSamplesReducer
    extends Reducer<VIntWritable, VLongWritable, VIntWritable, VLongWritable> {

    private int dataDimFrom;
    private int dataDimTo;
    private int universeSize;
    
    // hint
    private VLongWritable mapOutValue = new VLongWritable();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        universeSize = dataDimTo - dataDimFrom + 1;
    }
    
    @Override
    public void reduce(
            VIntWritable position,
            Iterable<VLongWritable> compositions,
            Context context) throws IOException, InterruptedException {
        boolean isFirst = true;
        long samplesCount = 0;
        final double magicNum = 1000000;
        for (VLongWritable composition : compositions) {
            samplesCount += composition.get();
        }
        mapOutValue.set(samplesCount);
        context.write(position, mapOutValue);
    }
    
}
