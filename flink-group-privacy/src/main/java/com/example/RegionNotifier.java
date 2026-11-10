package com.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.util.function.Consumer;

final class RegionNotifier implements FlatMapFunction<
        Tuple4<String, Integer, Long, double[]>,
        Tuple3<String, String, String>> {

    private final RegionGrid grid;
    private final String[] users;

    RegionNotifier(RegionGrid grid, String[] users) {
        this.grid = grid;
        this.users = users;
    }

    @Override
    public void flatMap(
            Tuple4<String, Integer, Long, double[]> value,
            Collector<Tuple3<String, String, String>> out) {
        for (RegionType type : grid.classifyGroup(value.f3)) {
            notifyUsers(value.f0, value.f2, type, users, out::collect);
        }
    }

    static void notifyUsers(
            String sender, long sequence, RegionType region, String[] users,
            Consumer<Tuple3<String, String, String>> notify) {
        if (region == RegionType.NORMAL) {
            return;
        }

        String event = "IN_" + region.name();
//        if (region == RegionType.NEIGHBOR_PRIVATE) {
//            System.out.printf(
//                    "NEIGHBOR_PRIVATE> user=%s, sequence=%d%n",
//                    sender, sequence);
//        }
        for (String recipient : users) {
            if (!sender.equals(recipient)) {
                notify.accept(Tuple3.of(sender, recipient, event));
            }
        }
    }
}