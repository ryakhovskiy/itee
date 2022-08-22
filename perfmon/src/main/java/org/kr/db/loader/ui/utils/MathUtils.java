package org.kr.db.loader.ui.utils;

/**
 * Created bykron 11.12.2014.
 */
public class MathUtils {

    public static double getTrapeziumSquare(double a, double b, double h) {
        double m = (a + b) / 2;
        return m * h;
    }

    public static double getTrapeziumIntegral(double[] points, double step) {
        double square = 0;
        for (int i = 1; i < points.length; i++)
            square += getTrapeziumSquare(points[i-1], points[i], step);
        return square;
    }

    public static long getTrapeziumSquare(long a, long b, long h) {
        long m = (a + b) / 2;
        return m * h;
    }

    public static long getTrapeziumIntegral(long[] points, long step) {
        long square = 0;
        for (int i = 1; i < points.length; i++)
            square += getTrapeziumSquare(points[i-1], points[i], step);
        return square;
    }
}
