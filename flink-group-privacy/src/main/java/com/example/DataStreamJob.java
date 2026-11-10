package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.util.Collector;

import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class DataStreamJob {
    private static final int MAX_RECORDS_PER_USER = 100;
    private static final long USER_TO_USER_DELAY_MS = 15;
    private static final long USER_TO_DME_DELAY_MS = 15;
    private static final long MAX_INPUT_FILES = 1000;
    private static final boolean USER_NOISE = true; //local privacy -> true, central privacy -> false

    public static void main(String[] args) throws Exception {
        Path logPath = Path.of("outputs", "group-privacy-expr.txt");
        Files.createDirectories(logPath.getParent());

        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;

        try (PrintStream log = new PrintStream(
                Files.newOutputStream(logPath), true, StandardCharsets.UTF_8)) {
            PrintStream consoleAndLogOut = tee(originalOut, log);
            PrintStream consoleAndLogErr = tee(originalErr, log);

            System.setOut(consoleAndLogOut);
            System.setErr(consoleAndLogErr);

            try {
                runJob(args);
            } catch (Exception | Error failure) {
                failure.printStackTrace(log);
                throw failure;
            } finally {
                consoleAndLogOut.flush();
                consoleAndLogErr.flush();
                System.setOut(originalOut);
                System.setErr(originalErr);
            }
        }
    }
    private static PrintStream tee(PrintStream console, PrintStream log) {
        return new PrintStream(new OutputStream() {
            @Override
            public void write(int value) {
                console.write(value);
                log.write(value);
            }

            @Override
            public void write(byte[] bytes, int offset, int length) {
                console.write(bytes, offset, length);
                log.write(bytes, offset, length);
            }

            @Override
            public void flush() {
                console.flush();
                log.flush();
            }
        }, true, StandardCharsets.UTF_8);
    }
    private static void runJob(String[] args) throws Exception {
        boolean userNoise = USER_NOISE;

        Map<String, String> filesByUser = UserFiles.discover(Path.of("inputs"), MAX_INPUT_FILES);

        String[] users = filesByUser.keySet().toArray(new String[0]);

        RegionGrid grid = RegionGrid.fromFiles(
                MAX_RECORDS_PER_USER,
                filesByUser.values().toArray(new String[0]));


        Configuration config = new Configuration();
        config.setString("taskmanager.memory.network.min", "256 mb");
        config.setString("taskmanager.memory.network.max", "256 mb");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);

        List<DataStream<Tuple4<String, Integer, Long, double[]>>> gpsStreams =
                new ArrayList<>();

        for (Map.Entry<String, String> entry : filesByUser.entrySet()) {
            gpsStreams.add(GpsInput.readUser(
                    env, entry.getKey(), entry.getValue(), MAX_RECORDS_PER_USER));
        }

        DataStream<Tuple4<String, Integer, Long, double[]>> gps =
                unionAll(gpsStreams)
                        .filter(row -> grid.isWithinBounds(row.f3))
                        .name("Filter outlier groups");

        TypeInformation<Tuple4<String, Integer, Long, double[]>> groupType =
                GpsInput.GROUP_TYPE;

        TypeInformation<Tuple2<Boolean, Tuple4<String, Integer, Long, double[]>>>
                dmeInputType = Types.TUPLE(Types.BOOLEAN, groupType);

        TypeInformation<Tuple3<String, String, String>> notificationType =
                Types.TUPLE(Types.STRING, Types.STRING, Types.STRING);

        gps.sinkTo(new OriginalTargetSink(grid))
                .name("Record original TARGET groups")
                .setParallelism(1);

        DataStream<Tuple2<Boolean, Tuple4<String, Integer, Long, double[]>>> dmeInput;
        DataStream<Tuple3<String, String, String>> notifications;

        if (userNoise) {
            OutputTag<Tuple3<String, String, String>> notificationTag =
                    new OutputTag<>("user-region-notifications", notificationType);

            var prepared = gps.process(
                            new UserPrivacy(grid, users, notificationTag), dmeInputType)
                    .name("Protect groups and notify other users");
            dmeInput = prepared;
            notifications = prepared.getSideOutput(notificationTag);
        } else {
            dmeInput = gps.map(row -> Tuple2.of(
                            grid.classifyGroup(row.f3).contains(RegionType.PRIVATE),
                            row))
                    .returns(dmeInputType)
                    .name("Mark private groups before DME");

            notifications = gps.flatMap(new RegionNotifier(grid, users))
                    .returns(notificationType);
        }

        var timestampedDmeInput = dmeInput.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple2<Boolean, Tuple4<String, Integer, Long, double[]>>>noWatermarks()
                        .withTimestampAssigner((record, previousTimestamp) ->
                                System.currentTimeMillis()));

        NetworkDelay.apply(timestampedDmeInput, USER_TO_DME_DELAY_MS)
                .name("Simulated user-to-DME network delay")
                .transform("DME", groupType, new DME(users, grid, userNoise))
                .setParallelism(1)
                .sinkTo(new DataConsumer(grid))
                .name("Central data consumer")
                .setParallelism(1);

        notifications = NetworkDelay.apply(notifications, USER_TO_USER_DELAY_MS)
                .name("Simulated user-to-user network delay");

        if (users.length >= 2) {
            var notificationSample = notifications
                    .map(new RichMapFunction<
                            Tuple3<String, String, String>,
                            Tuple3<String, String, String>>() {
                        @Override
                        public Tuple3<String, String, String> map(
                                Tuple3<String, String, String> message) {
                            getRuntimeContext().getLongCounter("sent:" + message.f0).add(1L);
                            getRuntimeContext().getLongCounter("received:" + message.f1).add(1L);
                            return message;
                        }
                    })
                    .returns(notificationType)
                    .filter(new FilterFunction<Tuple3<String, String, String>>() {
                        private int printed;

                        @Override
                        public boolean filter(Tuple3<String, String, String> message) {
                            return printed < 10 && ++printed <= 10;
                        }
                    });

            //notificationSample.print("Notification sample");
        } else {
            System.out.println("One Data Source Only!");
            notifications.print("Notification");
        }

        System.out.println("Noise mode: " + (userNoise ? "limited cooperative" : "fully cooperative"));
        java.nio.file.Files.createDirectories(java.nio.file.Path.of("outputs"));
        System.out.println("Num_Data_Sources: " + users.length);
        env.execute("GPS region notifications");


        try (var rows = Files.lines(Path.of("outputs", "original-target.csv"))) {
            long totalTargetPatterns = rows.skip(1).count();
            System.out.printf(
                    "Original Data> All users generated %d target patterns.%n",
                    totalTargetPatterns);
        }
        System.out.println("Expr. End");

//        for (String user : users) {
//            Long sent = result.getAccumulatorResult("sent:" + user);
//            Long received = result.getAccumulatorResult("received:" + user);
//
//            System.out.printf("%s\t%d\t%d%n",
//                    user,
//                    sent == null ? 0L : sent,
//                    received == null ? 0L : received);
//        }
    }
    private static boolean parseUserNoise(String[] args) {
        if (args.length == 0 || (args.length == 2
                && "--noise-mode".equals(args[0]) && "dme".equals(args[1]))) {
            return false;
        }
        if (args.length == 2 && "--noise-mode".equals(args[0]) && "user".equals(args[1])) {
            return true;
        }
        throw new IllegalArgumentException("Usage: [--noise-mode dme|user]");
    }
    private static <T> DataStream<T> unionAll(List<DataStream<T>> streams) {
        DataStream<T> first = streams.get(0);
        if (streams.size() == 1) {
            return first;
        }
        @SuppressWarnings("unchecked")
        DataStream<T>[] remaining =
                (DataStream<T>[]) new DataStream<?>[streams.size() - 1];
        streams.subList(1, streams.size()).toArray(remaining);
        return first.union(remaining);
    }
}
