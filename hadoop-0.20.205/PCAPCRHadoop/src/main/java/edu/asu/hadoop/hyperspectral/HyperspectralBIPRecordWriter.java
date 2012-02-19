/** Altai State University, 2012 */
package edu.asu.hadoop.hyperspectral;

import edu.asu.hadoop.DoubleArrayWritable;
import edu.asu.hadoop.ExtendedArrayWritable;
import java.io.IOException;
import java.io.DataOutputStream;
import org.apache.hadoop.mapreduce.*;

import edu.asu.hadoop.IntArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


/**
 * Save hyperspectral data in BIP format (Band interleaved by pixel)
 * in Hadoop 0.20.205 (RecordWriter)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class HyperspectralBIPRecordWriter<K,V>
    extends RecordWriter<K,V> {

    private DataOutputStream out;

//    private long numLines;
//    private long numPixPerLine;
//    private int numBands;
    private short precision;
    private boolean isLittleEndian;
    private boolean isUnsigned;
    private boolean isRoundDownward;

    HyperspectralBIPRecordWriter(
            DataOutputStream out,
//            long numLines,
//            long numPixPerLine,
//            int numBands,
            short precision,
            boolean isLittleEndian,
            boolean isUnsigned,
            boolean isRoundDownward) {
        this.out = out;
//        this.numBands = numBands;
//        this.numLines = numLines;
        this.isLittleEndian = isLittleEndian;
        this.isUnsigned = isUnsigned;
//        this.numPixPerLine = numPixPerLine;
        this.precision = precision;
        this.isRoundDownward = isRoundDownward;
    }

    @Override
    public void write(Object key, Object value)
            throws IOException {
        if (value == null) {
            throw new RuntimeException("Null value in input");
            //return;
        }
        if (!(value instanceof IntArrayWritable) && !(value instanceof DoubleArrayWritable)) {
            throw new RuntimeException("Bad value format in input");
            //return;
        }
        ExtendedArrayWritable<? extends Writable> valueArray
                = (ExtendedArrayWritable<? extends Writable>) value;
        for (int band = 0; band < valueArray.length(); band++) {
            //NOTE: In Java all of binary is in Big Endian
            Writable pixelValueBase = valueArray.get(band);
            int pixelValue;
            if (pixelValueBase instanceof IntWritable) {
                pixelValue = ((IntWritable) pixelValueBase).get();
            } else if (pixelValueBase instanceof DoubleWritable) {
                double pixelValueDouble = ((DoubleWritable) pixelValueBase).get();
                int maxValue, minValue;
                if (precision == 1) {
                    maxValue = isUnsigned ? Byte.MAX_VALUE*2 + 1 : Byte.MAX_VALUE;
                    minValue = isUnsigned ? 0 : Byte.MIN_VALUE;
                } else if (precision == 2) {
                    maxValue = isUnsigned ? Short.MAX_VALUE*2 + 1 : Short.MAX_VALUE;
                    minValue = isUnsigned ? 0 : Short.MIN_VALUE;
                } else if (precision == 4 && !isUnsigned){
                    maxValue = Integer.MAX_VALUE;
                    minValue = Integer.MIN_VALUE;
                } else {
                    throw new RuntimeException("Bad data format settings");
                    //return;
                }
                pixelValue = isRoundDownward ? (int) Math.floor(pixelValueDouble) :  (int) pixelValueDouble;
                if (pixelValue < minValue) {
                    pixelValue = minValue;
                } else if (pixelValue > maxValue) {
                    pixelValue = maxValue;
                }
            } else {
                throw new RuntimeException("Bad value format in input");
                //return;
            }
            
            byte[] pixelBuffer = new byte[precision];
            if (precision == 1) {
                pixelBuffer[0] = (byte)(pixelValue & 0xFF);
            } else if (precision == 2) {
                pixelBuffer[isLittleEndian ? 0 : 1] = (byte)(pixelValue & 0xFF);
                pixelBuffer[isLittleEndian ? 1 : 0] = (byte)((pixelValue & 0xFF00) >> 8);
            } else if (precision == 4 && !isUnsigned) {
                pixelBuffer[isLittleEndian ? 0 : 3] = (byte)(pixelValue & 0xFF);
                pixelBuffer[isLittleEndian ? 1 : 2] = (byte)((pixelValue & 0xFF00) >> 8);
                pixelBuffer[isLittleEndian ? 2 : 1] = (byte)((pixelValue & 0xFF0000) >> 16);
                pixelBuffer[isLittleEndian ? 3 : 0] = (byte)((pixelValue & 0xFF000000) >> 24);
            } else {
                throw new RuntimeException("Bad value format in input");
                //return;
            }
//            for (int j = 0; j < pixelBuffer.length; j++) {
//                System.out.format("%02X ", pixelBuffer[j]);
//            }
//            System.out.println();
            out.write(pixelBuffer);
        }
    }

    @Override
    public void close(TaskAttemptContext context)
            throws IOException {
        out.close();
    }

}
