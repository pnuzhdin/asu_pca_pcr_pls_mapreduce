package edu.asu.hadoop;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;


/**
 * Расширяемый ArrayWritable
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 *
 * @param <V>
 */
public class ExtendedArrayWritable<V extends Writable>
    extends ArrayWritable {
    
    public final int length;

    public ExtendedArrayWritable(Class<? extends Writable> valueClass) {
        super(valueClass);
        this.length = 0;
    }

    public ExtendedArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
        super(valueClass, values);
        this.length = values.length;
    }

    @Override
    public void set(Writable[] values) {
        super.set(values);
    }
    
    public V get(int idx) {
        return (V) get()[idx];
    }
    
}
