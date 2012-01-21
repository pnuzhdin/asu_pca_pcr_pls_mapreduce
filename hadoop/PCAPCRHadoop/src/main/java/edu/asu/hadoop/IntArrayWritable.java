package edu.asu.hadoop;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;


/**
 *
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class IntArrayWritable
    extends ArrayWritable {
    
    public final int length;

    public IntArrayWritable() {
        super(IntWritable.class);
        length = 0;
    }

    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
        length = values.length;
    }
    
    public void set(IntWritable[] values) {
        super.set(values);
    }
    
    public IntWritable get(int idx) {
        return (IntWritable) get()[idx];
    }
    
    public int[] getVector(int from, int to) {
        int sz = to - from + 1;
        int[] vector = new int[sz];
        for (int i = from; i <= to; i++) {
            vector[i-from] = get(i).get();
        }
        return vector;
    }
  
}
