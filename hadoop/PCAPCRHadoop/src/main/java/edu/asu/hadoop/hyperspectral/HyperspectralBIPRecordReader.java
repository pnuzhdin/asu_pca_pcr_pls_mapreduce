/** Altai State University, 2011 */
package edu.asu.hadoop.hyperspectral;

import java.io.IOException;
import java.io.DataInputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;

import edu.asu.hadoop.IntArrayWritable;


/**
 * Load hyperspectral data in BIP format (Band interleaved by pixel)
 * in Hadoop 0.21.0 (RecordReader)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class HyperspectralBIPRecordReader
    implements RecordReader<LongWritable, IntArrayWritable> {

    //NOTE: Used https://github.com/RIPE-NCC/hadoop-pcap/blob/master/hadoop-pcap-lib/src/main/java/net/ripe/hadoop/pcap/io/reader/PcapRecordReader.java as a template

//    private long numLines;
//    private long numPixPerLine;
    private int numBands;
    private short precision;
    private boolean isLittleEndian;
    private boolean isUnsigned;

    Seekable baseStream;
	DataInputStream stream;
	Reporter reporter;

	long lastkey = 0;
	long start, end;

    public HyperspectralBIPRecordReader(
            long start,
            long end,
            Seekable baseStream,
            DataInputStream stream,
            Reporter reporter,
//            long numLines,
//            long numPixPerLine,
            int numBands,
            short precision,
            boolean isLittleEndian,
            boolean isUnsigned)
            throws IOException {
        this.start = start;
        this.end = end;
        this.baseStream = baseStream;
        this.stream = stream;
        this.reporter = reporter;
        this.numBands = numBands;
//        this.numLines = numLines;
        this.isLittleEndian = isLittleEndian;
        this.isUnsigned = isUnsigned;
//        this.numPixPerLine = numPixPerLine;
        this.precision = precision;
    }

    public boolean next(LongWritable key, IntArrayWritable value)
            throws IOException {
        byte[] pixelBuffer = new byte[precision];
        for (int band = 0; band < numBands; band++) {
            if (stream.read(pixelBuffer) == precision) {
                //NOTE: In Java all of binary is in Big Endian
                int pixelValue;
                if (precision == 1) {
                    pixelValue = isUnsigned ? pixelBuffer[0] & 0xFF : pixelBuffer[0];
                } else if (precision == 2) {
                    byte b1 = pixelBuffer[isLittleEndian ? 1 : 0];
                    byte b2 = pixelBuffer[isLittleEndian ? 0 : 1];
                    pixelValue = ((isUnsigned ? b1 & 0xFF : b1) << 8) | (b2 & 0xFF);
                } else if (precision == 4 && !isUnsigned) {
                    byte b1 = pixelBuffer[isLittleEndian ? 3 : 0];
                    byte b2 = pixelBuffer[isLittleEndian ? 2 : 1];
                    byte b3 = pixelBuffer[isLittleEndian ? 1 : 2];
                    byte b4 = pixelBuffer[isLittleEndian ? 0 : 3];
                    pixelValue = ((b1 & 0xFF) << 24) | ((b2 & 0xFF) << 16) | ((b3 & 0xFF) << 8) | (b4 & 0xFF);
                } else {
                    //Take French leave
                    return false;
                }
                value.get(band).set(pixelValue);
            } else {
                return false;
            }
        }
        
        key.set(++lastkey);

        reporter.setStatus("Read " + getPos() + " of " + end + " bytes");
        reporter.progress();

        return true;
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public IntArrayWritable createValue() {
        IntWritable[] buffer = new IntWritable[numBands];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = new IntWritable();
        }
        return new IntArrayWritable(buffer);
    }

    public long getPos() throws IOException {
        return baseStream.getPos();
    }

    public void close() throws IOException {
        stream.close();
    }

    public float getProgress() throws IOException {
        if (start == end) {
            return 0;
        }
        return Math.min(1.0f, (getPos() - start) / (float) (end - start));
    }

}
