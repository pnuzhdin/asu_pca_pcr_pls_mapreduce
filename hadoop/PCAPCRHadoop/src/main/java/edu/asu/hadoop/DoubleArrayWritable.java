package edu.asu.hadoop;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;


/**
 *
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class DoubleArrayWritable
    extends ArrayWritable {
    
    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }

    public DoubleArrayWritable(DoubleWritable[] values) {
        super(DoubleWritable.class, values);
    }
    
    public void set(DoubleWritable[] values) {
        super.set(values);
    }
    
    public DoubleWritable get(int idx) {
        return (DoubleWritable) get()[idx];
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
