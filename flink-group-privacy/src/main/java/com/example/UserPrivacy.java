package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

final class UserPrivacy extends ProcessFunction<
        Tuple4<String, Integer, Long, double[]>,
        Tuple2<Boolean, Tuple4<String, Integer, Long, double[]>>> {

    private final RegionGrid grid;
    private final String[] users;
    private final OutputTag<Tuple3<String, String, String>> notificationTag;

    UserPrivacy(RegionGrid grid, String[] users,
                OutputTag<Tuple3<String, String, String>> notificationTag) {
        this.grid = grid;
        this.users = users.clone();
        this.notificationTag = notificationTag;
    }

    @Override
    public void processElement(
            Tuple4<String, Integer, Long, double[]> value,
            Context context,
            Collector<Tuple2<Boolean, Tuple4<String, Integer, Long, double[]>>> out) {

        var types = grid.classifyGroup(value.f3);
        boolean privateValue = types.contains(RegionType.PRIVATE);
        var outgoing = privateValue ? LaplaceNoise.apply(value, grid) : value;

        if (privateValue) {
            System.out.printf(
                    "User noise added> user=%s, sequence=%d, recordId=%d, original=%s, protected=%s%n",
                    outgoing.f0,
                    outgoing.f2,
                    outgoing.f1,
                    Arrays.toString(value.f3),
                    Arrays.toString(outgoing.f3));
        }

        for (RegionType type : types) {
            RegionNotifier.notifyUsers(value.f0, value.f2, type, users,
                    message -> context.output(notificationTag, message));
        }

        out.collect(Tuple2.of(privateValue, outgoing));
    }
}