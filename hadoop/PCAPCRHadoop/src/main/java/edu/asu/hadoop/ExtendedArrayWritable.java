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

    public ExtendedArrayWritable(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    public ExtendedArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
        super(valueClass, values);
    }

    @Override
    public void set(Writable[] values) {
        super.set(values);
    }
    
    public V get(int idx) {
        return (V) get()[idx];
    }

    public int length() {
        return get().length;
    }
    
}
