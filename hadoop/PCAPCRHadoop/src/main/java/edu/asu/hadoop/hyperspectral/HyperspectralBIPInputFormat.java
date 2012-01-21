/** Altai State University, 2012 */
package edu.asu.hadoop.hyperspectral;

import java.io.IOException;
import java.io.DataInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;

import edu.asu.hadoop.IntArrayWritable;


/**
 * Load hyperspectral data in BIP format (Band interleaved by pixel)
 * in Hadoop 0.21.0 (FileInputFormat)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class HyperspectralBIPInputFormat
    extends FileInputFormat<LongWritable, IntArrayWritable> {

    //NOTE: Used https://github.com/RIPE-NCC/hadoop-pcap/blob/master/hadoop-pcap-lib/src/main/java/net/ripe/hadoop/pcap/io/PcapInputFormat.java as a template

//    /** Number of lines of data in the file */
//    public static final String NUMLINES =
//            "mapreduce.input.hyperspectraldatainputformat.numlines";

//    /** Number of pixels on a line */
//    public static final String NUMPIXPERLINE =
//            "mapreduce.input.hyperspectraldatainputformat.numpixperline";

    /** Number of bands in the file */
    public static final String NUMBANDS =
            "mapreduce.input.hyperspectraldatainputformat.numbands";

    /** Format string of data, as Python struct definition */
    public static final String DATAFORMAT =
            "mapreduce.input.hyperspectraldatainputformat.dataformat";

    /** "<H" - Little-endian unsigned short (2 byte) */
    public static final String DATAFORMAT_LE_USHORT = "<H";

    @Override
    public RecordReader<LongWritable, IntArrayWritable> getRecordReader(
            InputSplit split,
            JobConf job,
            Reporter reporter) throws IOException {
        short precision = 1;
        boolean isLittleEndian = false;
        boolean isUnsigned = false;
        if (DATAFORMAT_LE_USHORT.equals(job.get(DATAFORMAT))) {
            precision = 2;
            isLittleEndian = true;
            isUnsigned = true;
        }

        FileSplit fileSplit = (FileSplit)split;
		Path path = fileSplit.getPath();
		long start = 0L;
		long length = fileSplit.getLength();
        FileSystem fs = path.getFileSystem(job);
        FSDataInputStream baseStream = fs.open(path);
	    DataInputStream stream = baseStream;
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(path);
        if (codec != null) {
            stream = new DataInputStream(codec.createInputStream(stream));
        }

        return new HyperspectralBIPRecordReader(
                start,
                length,
                baseStream,
                stream,
                reporter,
//                job.getLong(NUMLINES, 1),
//                job.getLong(NUMPIXPERLINE, 1),
                job.getInt(NUMBANDS, 1),
                precision,
                isLittleEndian,
                isUnsigned);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        //NOTE: I think it's possible to process splittable  BIP file
        return false;
    }

//    /**
//     * Get the number of lines of data in the file
//     *
//     * @param job Job
//     * @return The number of lines of data in the file
//     */
//    public static long getNumLines(JobContext job) {
//        return job.getConfiguration().getLong(NUMLINES, 1);
//    }

//    /**
//     * Set the number of lines of data in the file
//     *
//     * @param job Job
//     * @param numLines The number of lines of data in the file
//     */
//    public static void setNumLines(JobContext job, long numLines) {
//        job.getConfiguration().setLong(NUMLINES, numLines);
//    }

//    /**
//     * Get the number of pixels on a line
//     *
//     * @param job Job
//     * @return The number of pixels on a line
//     */
//    public static long getNumPixelsPerLine(JobContext job) {
//        return job.getConfiguration().getLong(NUMPIXPERLINE, 1);
//    }
//
//    /**
//     * Set the number of lines of data in the file
//     *
//     * @param job Job
//     * @param numPixelsPerLine The number of lines of data in the file
//     */
//    public static void setNumPixelsPerLine(JobContext job, long numPixelsPerLine) {
//        job.getConfiguration().setLong(NUMPIXPERLINE, numPixelsPerLine);
//    }

    /**
     * Get the number of bands in the file
     *
     * @param job Job
     * @return The number of pixels on a line
     */
    public static int getNumBands(JobContext job) {
        return job.getConfiguration().getInt(NUMBANDS, 1);
    }

    /**
     * Set the number of bands in the file
     *
     * @param job Job
     * @param numBands The number of lines of data in the file
     */
    public static void setNumBands(JobContext job, int numBands) {
        job.getConfiguration().setInt(NUMBANDS, numBands);
    }

    /**
     * Get format string of data
     *
     * @param job Job
     * @return Data format
     */
    public static String getDataFormat(JobContext job) {
        return job.getConfiguration().get(DATAFORMAT);
    }

    /**
     * Set format string of data
     *
     * @param job Job
     * @param dataFormat Format string of data
     */
    public static void setDataFormat(JobContext job, String dataFormat) {
        job.getConfiguration().set(DATAFORMAT, dataFormat);
    }
    
}
