package com.avinash.engine.function;

import org.apache.spark.api.java.function.Function2;

import java.util.Optional;

public class SparkFunctions {
    public static final class VectorSum implements Function2<Double[], Double[], Double[]> {
        @Override
        public Double[] call(Double[] a, Double[] b) {
            Double[] sum= new Double[a.length];
            for(int i=0;i<a.length;i++){
                sum[0]= new DoubleSum().call(a[i],Optional.of(b[i])).orElse(0.0d);
            }
            return sum;
        }
    }

    public static final class DoubleSum implements Function2<Double, Optional<Double>, Optional<Double>> {
        @Override
        public Optional<Double> call(Double a,  Optional<Double> b) {
            return Optional.of(a + b.orElse(0.0d));
        }
    };
}
