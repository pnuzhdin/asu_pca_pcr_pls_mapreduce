/** Altai State University, 2011 */
package edu.asu.hadoop;

import org.apache.hadoop.conf.Configuration;

/**
 * Utils for Configuration (Hadoop 0.21.0)
 * @author Pavel Nuzhdin <pnzhdin@gmail.com>
 */
public final class ConfigurationUtils {
    
    public static void setVector(Configuration conf, String variableName, double[] vector) {
        if (vector == null) {
            conf.set(variableName, "");
            return;
        }
        String values = String.valueOf(vector.length);
        for (int i = 0; i < vector.length; i++) {
            values += " " + String.valueOf(vector[i]);
        }
        conf.set(variableName, values);
    }
    
    public static double[] getVector(Configuration conf, String variableName) {
        if (conf.get(variableName) == null || conf.get(variableName).isEmpty()) {
            return null;
        }
        String[] valuesStrs = conf.get(variableName).split(" ");
        double[] values = new double[valuesStrs.length - 1];
        for (int i = 1; i < valuesStrs.length; i++) {
            values[i-1] = Double.valueOf(valuesStrs[i]);
        }
        return values;
    }
    
    public static void setMatrix(Configuration conf, String variableName, double[][] matrix) {
        if (matrix == null) {
            conf.set(variableName, "");
            return;
        }
        String values = String.valueOf(matrix.length) + " " + String.valueOf(matrix[0].length);
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                values += " " + String.valueOf(matrix[i][j]);
            }
        }
        conf.set(variableName, values);
    }
    
    public static double[][] getMatrix(Configuration conf, String variableName) {
        if (conf.get(variableName) == null || conf.get(variableName).isEmpty()) {
            return null;
        }
        String[] valuesStrs = conf.get(variableName).split(" ");
        int rows = Integer.valueOf(valuesStrs[0]);
        int cols = Integer.valueOf(valuesStrs[1]);
        double[][] values = new double[rows][cols];
        int v = 2;
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++, v++) {
                values[i][j] = Double.valueOf(valuesStrs[v]);
            }
        }
        return values;
    }
    
}
