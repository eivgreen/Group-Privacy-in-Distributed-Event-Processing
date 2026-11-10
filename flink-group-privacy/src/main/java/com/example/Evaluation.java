package com.example;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashSet;
import java.util.Set;

final class Evaluation {
    private final Set<Tuple2<String, Long>> dmeTargets = new HashSet<>();
    private final Set<Tuple2<String, Long>> consumerTargets = new HashSet<>();

    void addDmeTarget(String user, long sequence) {
        dmeTargets.add(Tuple2.of(user, sequence));
    }

    void addConsumerTarget(String user, long sequence) {
        consumerTargets.add(Tuple2.of(user, sequence));
    }

    double f1Score() {
        long truePositives = 0;
        for (var id : consumerTargets) {
            if (dmeTargets.contains(id)) {
                truePositives++;
            }
        }
        long total = (long) dmeTargets.size() + consumerTargets.size();
        return total == 0 ? 0.0 : 2.0 * truePositives / total;
    }
}
