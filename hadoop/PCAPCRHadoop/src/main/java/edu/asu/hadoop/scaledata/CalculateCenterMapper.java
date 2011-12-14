/** Altai State University, 2011 */
package edu.asu.hadoop.scaledata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.asu.hadoop.DoubleArrayWritable;


/**
 * Centering Data for Hadoop 0.21.0 (Mapper)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class CalculateCenterMapper
    extends Mapper<LongWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {
    
    private int dataDimFrom;
    private int dataDimTo;
    private long samplesCount;
    private int universeSize;
    
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dataDimFrom = conf.getInt("dataDimFrom", 0);
        dataDimTo = conf.getInt("dataDimTo", 0);
        samplesCount = conf.getLong("samplesCount", 0);
        universeSize = dataDimTo - dataDimFrom + 1;
    }
    
    @Override
    public void map(
            LongWritable key,
            DoubleArrayWritable array,
            Context context) throws IOException, InterruptedException {
        DoubleWritable[] outArray = new DoubleWritable[universeSize];
        for (int c = 0; c < universeSize; c++) {
            outArray[c] = new DoubleWritable(array.get(c+dataDimFrom).get() / samplesCount);
            //context.write(new IntWritable(c), new DoubleWritable(array.get(c+dataDimFrom).get() / samplesCount));
        }
        context.write(new IntWritable(1), new DoubleArrayWritable(outArray));
    }
    
}
