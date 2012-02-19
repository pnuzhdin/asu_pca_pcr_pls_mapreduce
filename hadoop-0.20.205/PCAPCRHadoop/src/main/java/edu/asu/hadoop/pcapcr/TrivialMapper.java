/** Altai State University, 2011 */
package edu.asu.hadoop.pcapcr;

import java.io.IOException;
import edu.asu.hadoop.IntArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Trivial Mapper for Hadoop 0.20.205 (key -> key, value -> value)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class TrivialMapper
    extends Mapper<LongWritable, IntArrayWritable, LongWritable, IntArrayWritable> {
    
    @Override
    public void map(
            LongWritable key,
            IntArrayWritable array,
            Context context) throws IOException, InterruptedException {
        context.write(key, array);
    }
    
}
