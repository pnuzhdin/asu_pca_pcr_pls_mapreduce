/** Altai State University, 2011 */
package edu.asu.hadoop.hyperspectral;

import java.io.IOException;
import java.io.DataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;

import edu.asu.hadoop.IntArrayWritable;


/**
 * Save hyperspectral data in BIP format (Band interleaved by pixel)
 * in Hadoop 0.21.0 (RecordWriter)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class HyperspectralBIPRecordWriter
    implements RecordWriter<LongWritable, IntArrayWritable> {

    private DataOutputStream out;

//    private long numLines;
//    private long numPixPerLine;
//    private int numBands;
    private short precision;
    private boolean isLittleEndian;
    private boolean isUnsigned;

    HyperspectralBIPRecordWriter(
            DataOutputStream out,
//            long numLines,
//            long numPixPerLine,
//            int numBands,
            short precision,
            boolean isLittleEndian,
            boolean isUnsigned) {
        this.out = out;
//        this.numBands = numBands;
//        this.numLines = numLines;
        this.isLittleEndian = isLittleEndian;
        this.isUnsigned = isUnsigned;
//        this.numPixPerLine = numPixPerLine;
        this.precision = precision;
    }

    public void write(LongWritable key, IntArrayWritable value)
            throws IOException {
        if (value == null) {
            return;
        }
        for (int band = 0; band < value.length; band++) {
            //NOTE: In Java all of binary is in Big Endian
            int pixelValue = value.get(band).get();
            byte[] pixelBuffer = new byte[precision];
            if (precision == 1) {
                pixelBuffer[0] = (byte)(pixelValue & 0xFF000000);
            } else if (precision == 2) {
                pixelBuffer[isLittleEndian ? 1 : 0] = (byte)(pixelValue & 0xFF000000);
                pixelBuffer[isLittleEndian ? 0 : 1] = (byte)((pixelValue & 0x00FF0000) << 8);
            } else if (precision == 4) {
                pixelBuffer[isLittleEndian ? 3 : 0] = (byte)(pixelValue & 0xFF000000);
                pixelBuffer[isLittleEndian ? 2 : 1] = (byte)((pixelValue & 0x00FF0000) << 8);
                pixelBuffer[isLittleEndian ? 1 : 2] = (byte)((pixelValue & 0x0000FF00) << 16);
                pixelBuffer[isLittleEndian ? 0 : 3] = (byte)((pixelValue & 0x000000FF) << 24);
            }
            out.write(pixelBuffer);
        }
    }

    public void close(Reporter reporter)
            throws IOException {
        out.close();
    }

}
