package com.example;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.concurrent.ThreadLocalRandom;

final class LaplaceNoise {
    private static final double EPSILON = 1.0;

    private LaplaceNoise() {}

    static Tuple4<String, Integer, Long, double[]> apply(
            Tuple4<String, Integer, Long, double[]> record,
            RegionGrid grid) {
        double[] adjusted = record.f3.clone();
        int index = ThreadLocalRandom.current().nextInt(adjusted.length);
        double scale = grid.valueSpan(index) / (EPSILON / 2.0);
        adjusted[index] += sample(scale);
//        System.out.printf(
//                "Laplace noise> user=%s, sequence=%d, column=%d, min=%s, max=%s, scale=%s%n",
//                record.f0, record.f2, index + 3,
//                grid.minValue(index), grid.maxValue(index), scale);
        return Tuple4.of(record.f0, record.f1, record.f2, adjusted);
    }

    private static double sample(double scale) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        double magnitude = -scale * Math.log1p(-random.nextDouble());
        return random.nextBoolean() ? magnitude : -magnitude;
    }
}