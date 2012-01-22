/** Altai State University, 2012 */
package edu.asu.hadoop.hyperspectral;

import java.io.InputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.asu.hadoop.IntArrayWritable;


/**
 * Load hyperspectral data in BIP format (Band interleaved by pixel)
 * in Hadoop 0.21.0 (RecordReader)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class HyperspectralBIPRecordReader
    extends RecordReader<LongWritable, IntArrayWritable> {

    //NOTE: Used https://github.com/RIPE-NCC/hadoop-pcap/blob/master/hadoop-pcap-lib/src/main/java/net/ripe/hadoop/pcap/io/reader/PcapRecordReader.java as a template

//    private long numLines;
//    private long numPixPerLine;
    private int numBands;
    private short precision;
    private boolean isLittleEndian;
    private boolean isUnsigned;

	private InputStream in;
    private FileSplit fileSplit;

	private long lastkey = 0;
	private long start, end, position;

    private LongWritable key;
    private IntArrayWritable value;

    public HyperspectralBIPRecordReader(
//            long numLines,
//            long numPixPerLine,
            int numBands,
            short precision,
            boolean isLittleEndian,
            boolean isUnsigned)
            throws IOException {
        this.numBands = numBands;
//        this.numLines = numLines;
        this.isLittleEndian = isLittleEndian;
        this.isUnsigned = isUnsigned;
//        this.numPixPerLine = numPixPerLine;
        this.precision = precision;
    }

    private IntArrayWritable newValue() {
        IntWritable[] buffer = new IntWritable[numBands];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = new IntWritable();
        }
        return new IntArrayWritable(buffer);
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    @Override
    public float getProgress() throws IOException {
        if (start == end) {
            return 0;
        }
        return Math.min(1.0f, (position - start) / (float) (end - start));
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.fileSplit = (FileSplit) split;
        this.start = fileSplit.getStart();
        this.end = fileSplit.getLength();
        this.position = 0;
        Path path = fileSplit.getPath();
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream dis = fs.open(path);
        CompressionCodecFactory codecs = new CompressionCodecFactory(conf);
        CompressionCodec codec = codecs.getCodec(path);
        if (codec != null) {
            this.in = codec.createInputStream(dis);
        } else {
            this.in = dis;
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        key = new LongWritable();
        value = newValue();
        byte[] pixelBuffer = new byte[precision];
        for (int band = 0; band < numBands; band++) {
            if (in.read(pixelBuffer) == precision) {
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

        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public IntArrayWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

}
