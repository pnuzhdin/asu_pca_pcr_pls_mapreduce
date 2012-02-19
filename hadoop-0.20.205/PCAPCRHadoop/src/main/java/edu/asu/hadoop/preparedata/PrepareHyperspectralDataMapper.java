/** Altai State University, 2011 */
package edu.asu.hadoop.preparedata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.asu.hadoop.DoubleArrayWritable;
import edu.asu.hadoop.IntArrayWritable;


/**
 * Prepare hyperspectral data for Hadoop 0.20.205 (Mapper)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class PrepareHyperspectralDataMapper
    extends Mapper<LongWritable, IntArrayWritable, VLongWritable, DoubleArrayWritable> {
    
    private int size;
    
    // hint
    private DoubleWritable[] doubleArray;
    private DoubleArrayWritable mapperOutArray = new DoubleArrayWritable();
    private VLongWritable mapOutKey = new VLongWritable();
    
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        size = conf.getInt("dataDimSize", 0);
        doubleArray = new DoubleWritable[size];
        for (int i = 0; i < size; i++) {
            doubleArray[i] = new DoubleWritable();
        }
    }
    
    @Override
    public void map(
            LongWritable key,
            IntArrayWritable row,
            Context context) throws IOException, InterruptedException {
        for (int i = 0; i < size; i++) {
            doubleArray[i].set(row.get(i).get());
        }
        mapperOutArray.set(doubleArray);
        mapOutKey.set(key.get());
        context.write(mapOutKey, mapperOutArray);
    }
    
}
