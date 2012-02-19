/** Altai State University, 2011 */
package edu.asu.hadoop;

import java.util.Locale;
import org.apache.hadoop.io.Text;


/**
 * Point in multidimensional space
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public class Point {
    
    public final static String fieldSeparator = ",";
    public final static String doubleTxtFormat = "%.11f";
    
    private Object[] vector;
    
    private int dataStart;
    private int dataEnd;
    
    public Object get(int i) {
        return vector[i];
    }
    
    public void set(int i, Object obj) {
        vector[i] = obj;
    }
    
    public int size() {
        return vector.length;
    }

    public int getDataEnd() {
        return dataEnd;
    }

    public void setDataEnd(int dataEnd) {
        this.dataEnd = dataEnd;
    }

    public int getDataStart() {
        return dataStart;
    }

    public void setDataStart(int dataStart) {
        this.dataStart = dataStart;
    }
    
    public double distanceEuclidian(Point b) {
        double p = 0;

        for (int i = dataStart; i <= this.dataEnd; i++) { // Euclidian
            p += Math.pow((Double) this.get(i) - (Double) b.get(i), 2);
        }
        return Math.sqrt(p);
    }
    
    public double distanceChebyshev(Point b) {
        double p = 0;

        for (int i = dataStart; i <= this.dataEnd; i++) { // Chebyshev
            double d = Math.abs((Double) this.get(i) - (Double) b.get(i));
            if(d > p) p = d;
        }
        return p;
    }
    
    public double distanceManhattan(Point b) {
        double p = 0;

        for (int i = dataStart; i <= this.dataEnd; i++) { // Manhattan
            p += Math.abs((Double) this.get(i) - (Double) b.get(i));
        }
        return p;
    }

    public Point() {
    }
    
    public Point(int dataStart, int dataEnd, String[] row) {
        this.dataStart = dataStart;
        this.dataEnd = dataEnd;
        vector = new Object[row.length];
        for (int i = 0; i < row.length; i++) {
            if (i >= dataStart && i <= dataEnd) {
                vector[i] = Double.valueOf(row[i]);
            } else {
                vector[i] = row[i];
            }
        }
    }

    public Point(int dataStart, int dataEnd, int size) {
        this.dataStart = dataStart;
        this.dataEnd = dataEnd;
        vector = new Object[size];
    }

    public Point(int dataStart, int dataEnd, int size, double fillobj) {
        this.dataStart = dataStart;
        this.dataEnd = dataEnd;
        vector = new Object[size];
        for (int i = dataStart; i <= dataEnd; i++) {
            vector[i] = Double.valueOf(fillobj);
        }
//        for (int i = 0; i < size; i++) {
//            if (i >= dataStart && i <= dataEnd) {
//                vector[i] = fillobj;
//            } else {
//                vector[i] = null;
//            }
//        }
    }
    
    public Point(Point point, double fillobj) {
        this.dataStart = point.dataStart;
        this.dataEnd = point.dataEnd;
        vector = new Object[point.size()];
        System.arraycopy(point.vector, 0, vector, 0, point.size());
        for (int i = dataStart; i <= dataEnd; i++) {
            vector[i] = Double.valueOf(fillobj);
        }
    }

    public Point(int dataStart, int dataEnd, String row) {
        this(dataStart, dataEnd, row.split(fieldSeparator));
    }

    public Point(int dataStart, int dataEnd, Text txt) {
        this(dataStart, dataEnd, txt.toString());
    }
    
    public Object remove(int i) {
        Object old = vector[i];
        vector[i] = null;
        return old;
    }

    @Override
    public String toString() {
        String strval = "";
        for(int i = 0; i < this.size(); i++) {
            Object val = this.get(i);
            if (val == null) {
                continue;
            }
            if (i >= dataStart && i <= dataEnd) {
                strval += String.format(Locale.US, doubleTxtFormat, (Double) val);
            } else {
                strval += (String) val;
            }
            if (i != this.size() - 1) {
                strval += fieldSeparator;
            }
        }
        return strval;
    }

    public Text toText() {
        return new Text(toString());
    }
}
