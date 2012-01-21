/** Altai State University, 2011 */
package edu.asu.hadoop.hyperspectral;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;

import edu.asu.hadoop.IntArrayWritable;


/**
 * Save hyperspectral data in BIP format (Band interleaved by pixel)
 * in Hadoop 0.21.0 (FileOutputFormat)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class HyperspectralBIPOutputFormat
    extends FileOutputFormat<LongWritable, IntArrayWritable> {

//    /** Number of lines of data in the file */
//    public static final String NUMLINES =
//            "mapreduce.input.hyperspectraldatainputformat.numlines";

//    /** Number of pixels on a line */
//    public static final String NUMPIXPERLINE =
//            "mapreduce.input.hyperspectraldatainputformat.numpixperline";

//    /** Number of bands in the file */
//    public static final String NUMBANDS =
//            "mapreduce.input.hyperspectraldatainputformat.numbands";

    /** Format string of data, as Python struct definition */
    public static final String DATAFORMAT =
            "mapreduce.input.hyperspectraldatainputformat.dataformat";

    /** "<H" - Little-endian unsigned short (2 byte) */
    public static final String DATAFORMAT_LE_USHORT = "<H";

    @Override
    public RecordWriter<LongWritable, IntArrayWritable> getRecordWriter(
            FileSystem ignored,
            JobConf job,
            String name,
            Progressable progress) throws IOException {
        short precision = 1;
        boolean isLittleEndian = false;
        boolean isUnsigned = false;
        if (DATAFORMAT_LE_USHORT.equals(job.get(DATAFORMAT))) {
            precision = 2;
            isLittleEndian = true;
            isUnsigned = true;
        }

        if (!getCompressOutput(job)) {
            Path file = (Path) FileOutputFormat.getTaskOutputPath(job, name);
            FileSystem fs = file.getFileSystem(job);
            FSDataOutputStream fileOut = fs.create(file, progress);
            return new HyperspectralBIPRecordWriter(
                    fileOut,
//                    job.getLong(NUMPIXPERLINE, 1),
                    precision,
                    isLittleEndian,
                    isUnsigned);
        } else {
            Class codecClass = getOutputCompressorClass(job, DefaultCodec.class);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, job);
            Path file = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension());
            FileSystem fs = file.getFileSystem(job);
            FSDataOutputStream fileOut = fs.create(file, progress);
            return new HyperspectralBIPRecordWriter(
                    new DataOutputStream(codec.createOutputStream(fileOut)),
//                    job.getLong(NUMPIXPERLINE, 1),
                    precision,
                    isLittleEndian,
                    isUnsigned);
        }
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
