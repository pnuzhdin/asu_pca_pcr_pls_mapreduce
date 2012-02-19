package edu.asu.hadoop;

import org.apache.hadoop.io.IntWritable;


/**
 *
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class IntArrayWritable
    extends ExtendedArrayWritable<IntWritable> {
    
    public IntArrayWritable() {
        super(IntWritable.class);
    }

    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
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
