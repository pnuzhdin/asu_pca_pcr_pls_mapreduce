/** Altai State University, 2011 */
package edu.asu.hadoop.scaledata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.asu.hadoop.DoubleArrayWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;


/**
 * Calculate mean and STDEV Hadoop 0.21.0 (Mapper)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CalculateMeanAndSTDEVMapper
    extends Mapper<VLongWritable, DoubleArrayWritable, VIntWritable, DoubleArrayWritable> {
    
    private int dataDimFrom;
    private int dataDimTo;
    private long samplesCount;
    private int universeSize;
    
    // hint
    private DoubleWritable[] outArray;
    private VIntWritable outKey = new VIntWritable(1);
    private DoubleArrayWritable mapOutArray = new DoubleArrayWritable();
    
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        samplesCount = conf.getLong("samplesCount", 0);
        universeSize = dataDimTo - dataDimFrom + 1;
        outArray = new DoubleWritable[universeSize*2];
        for (int i = 0; i < universeSize*2; i++) {
            outArray[i] = new DoubleWritable();
        }
    }
    
    @Override
    public void map(
            VLongWritable key,
            DoubleArrayWritable array,
            Context context) throws IOException, InterruptedException {
        for (int c = 0; c < universeSize; c++) {
            outArray[c].set(array.get(c+dataDimFrom).get() / samplesCount);
        }
        for (int c = universeSize; c < universeSize*2; c++) {
            double val = array.get(c-universeSize+dataDimFrom).get();
            outArray[c].set((val*val) / samplesCount);
        }
        mapOutArray.set(outArray);
        context.write(outKey, mapOutArray);
    }
    
}
