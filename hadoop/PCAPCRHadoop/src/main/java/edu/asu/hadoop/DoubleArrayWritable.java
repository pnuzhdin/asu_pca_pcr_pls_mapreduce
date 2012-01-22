package edu.asu.hadoop;

import org.apache.hadoop.io.DoubleWritable;


/**
 *
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class DoubleArrayWritable
    extends ExtendedArrayWritable<DoubleWritable> {
    
    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }

    public DoubleArrayWritable(DoubleWritable[] values) {
        super(DoubleWritable.class, values);
    }
    
    public double[] getVector(int from, int to) {
        int sz = to - from + 1;
        double[] vector = new double[sz];
        for (int i = from; i <= to; i++) {
            vector[i-from] = get(i).get();
        }
        return vector;
    }
  
}
