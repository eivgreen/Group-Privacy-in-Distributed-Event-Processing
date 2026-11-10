package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ScheduledFuture;

final class DME extends AbstractStreamOperator<
        Tuple4<String, Integer, Long, double[]>>
        implements OneInputStreamOperator<
        Tuple2<Boolean, Tuple4<String, Integer, Long, double[]>>,
        Tuple4<String, Integer, Long, double[]>>,
        BoundedOneInput {

    private transient List<Long> bufferedSentAt;
    private transient ArrayDeque<StreamRecord<Tuple2<Boolean, Tuple4<String, Integer, Long, double[]>>>> deferred;

    private enum DmeMode {
        PHYSICAL,
        LOGICAL
    }
    private static final DmeMode DME_MODE = DmeMode.LOGICAL;

    private static final int RECORDS_PER_USER = 0;
    private static final int RECORDS_ALL_USER = 10;

    // window size
    private static final long MAX_WINDOW_WAIT_MS = 100L; //50ms

    // noise-record-count*2 is number of events in private patterns
    private static final int NOISE_RECORD_COUNT = 4;

    private transient BufferedWriter targetWriter;

    private final String[] users;

    private transient List<Tuple4<String, Integer, Long, double[]>> buffered;
    private transient Set<String> notifyingUsers;
    private transient Map<String, Integer> remainingByUser;

    private final RegionGrid grid;
    private transient long receivedGpsCount;
    private transient long targetCount;

    private transient ScheduledFuture<?> windowTimer;
    private transient long windowDeadline;
    private transient long windowGeneration;

    private final boolean userNoise;

    DME(String[] users, RegionGrid grid, boolean userNoise) {
        this.users = users.clone();
        this.grid = grid;
        this.userNoise = userNoise;
    }

    private void startWindowTimer() {
        windowDeadline = getProcessingTimeService().getCurrentProcessingTime()
                + MAX_WINDOW_WAIT_MS;

        long generation = ++windowGeneration;
        windowTimer = getProcessingTimeService().registerTimer(
                windowDeadline,
                timestamp -> {
                    if (windowTimer != null && generation == windowGeneration) {
                        flushAndReplay();
                    }
                });
    }

    private boolean windowIsFull() {
        return buffered.size() >= RECORDS_ALL_USER;
    }

    private void flushAndReplay() {
        do {
            flushWindow();
            replayDeferred();
        } while (windowIsFull());
    }

    @Override
    public void open() throws Exception {
        super.open();
        bufferedSentAt = new ArrayList<>();
        buffered = new ArrayList<>();
        notifyingUsers = new LinkedHashSet<>();
        remainingByUser = new HashMap<>();
        deferred = new ArrayDeque<>();
        targetWriter = Files.newBufferedWriter(Path.of("outputs", "dme-target.csv"));
        targetWriter.write("user,sequence");
        targetWriter.newLine();
    }
    @Override
    public void processElement(
            StreamRecord<Tuple2<Boolean, Tuple4<String, Integer, Long, double[]>>> element)
            throws IOException {
        long userSentAt = element.getTimestamp();
        var event = element.getValue();
        var record = event.f1;
        receivedGpsCount++;

        boolean receivedTarget = grid.classifyGroup(record.f3).contains(RegionType.TARGET);
        if (receivedTarget) {
            targetCount++;
            targetWriter.write("\"" + record.f0.replace("\"", "\"\"") + "\"," + record.f2);
            targetWriter.newLine();
        }

        if (userNoise) {
            emit(record, receivedTarget ? userSentAt : null);
            return;
        }

        var copied = Tuple2.of(event.f0,
                Tuple4.of(record.f0, record.f1, record.f2, record.f3.clone()));

        if (windowTimer != null
                && getProcessingTimeService().getCurrentProcessingTime() >= windowDeadline) {
            flushAndReplay();
        }

        accept(receivedTarget
                ? new StreamRecord<>(copied, userSentAt)
                : new StreamRecord<>(copied));

        if (windowIsFull()) {
            flushAndReplay();
        }
    }

    private void accept(StreamRecord<
            Tuple2<Boolean, Tuple4<String, Integer, Long, double[]>>> input) {

        var event = input.getValue();
        var gps = event.f1;
        Long sentAt = input.hasTimestamp() ? input.getTimestamp() : null;

        if (buffered.isEmpty()) {
            if (!event.f0) {
                emit(gps, input.hasTimestamp() ? input.getTimestamp() : null);
                return;
            }
            for (String user : users) {
                remainingByUser.put(user, RECORDS_PER_USER);
            }
            startWindowTimer();
        }

        Integer remaining = remainingByUser.get(gps.f0);
        if (remaining == null) {
            deferred.addLast(input);
            return;
        }

        buffered.add(gps);
        bufferedSentAt.add(sentAt);
        if (event.f0) {
            notifyingUsers.add(gps.f0);
        }

        if (remaining == 1) {
            remainingByUser.remove(gps.f0);
        } else {
            remainingByUser.put(gps.f0, remaining - 1);
        }
    }

    private void replayDeferred() {
        var pending = deferred;
        deferred = new ArrayDeque<>();

        while (!pending.isEmpty()) {
            accept(pending.removeFirst());

            if (windowIsFull()) {
                break;
            }
        }

        deferred.addAll(pending);
    }

    @Override
    public void endInput() {
        while (!buffered.isEmpty() || !deferred.isEmpty()) {
            if (!buffered.isEmpty()) {
                flushWindow();
            }
            replayDeferred();
        }
        System.out.printf("DME> Received %d records，%d target patterns detected.%n",
                receivedGpsCount, targetCount);
    }

    private void emit(
            Tuple4<String, Integer, Long, double[]> gps, Long sentAt) {
        output.collect(sentAt == null
                ? new StreamRecord<>(gps)
                : new StreamRecord<>(gps, sentAt));
    }

    private void flushWindow() {
        if (windowTimer != null) {
            windowTimer.cancel(false);
            windowTimer = null;
        }

        int receivedCount = buffered.size();
        int noiseCount = processWindow();

        for (int i = 0; i < receivedCount; i++) {
            emit(buffered.get(i), bufferedSentAt.get(i));
        }

//        System.out.printf(
//                "DME> Received %d records, added noise to %d records%n",
//                receivedCount, noiseCount);

        bufferedSentAt.clear();
        buffered.clear();
        remainingByUser.clear();
    }

    private int processWindow() {
        List<Integer> privateIndices = new ArrayList<>();
        List<Integer> nonPrivateIndices = new ArrayList<>();
        long logicalDelayMs = 0;

        for (int i = 0; i < buffered.size(); i++) {
            var record = buffered.get(i);
            if (DME_MODE == DmeMode.LOGICAL) {
                logicalDelayMs += ThreadLocalRandom.current().nextInt(10, 21);
            }
            if (grid.classifyGroup(record.f3).contains(RegionType.PRIVATE)) {
                privateIndices.add(i);
//                if (DME_MODE == DmeMode.LOGICAL) {
//                    logicalDelayMs += ThreadLocalRandom.current().nextInt(10, 21);
//                }
            } else {
                nonPrivateIndices.add(i);
            }
        }

        if (logicalDelayMs > 0) {
            try {
                Thread.sleep(logicalDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("DME logical delay interrupted", e);
            }
        }

        List<Integer> candidates;
        int privateCount = privateIndices.size();

        System.out.printf(
                "DME window> integration mode=%s, total=%d, potential_private=%d, non_Private=%d, "
                        + "num_events_in_private_patterns=%d%n",
                DME_MODE, buffered.size(), privateCount, nonPrivateIndices.size(),
                2*NOISE_RECORD_COUNT);

        if (privateCount >= NOISE_RECORD_COUNT) {
            candidates = privateIndices;
        } else if (privateCount == NOISE_RECORD_COUNT - 1) {
            candidates = nonPrivateIndices;
        } else {
            return 0;
        }

        if (candidates.isEmpty()) {
            return 0;
        }

        int index = candidates.get(
                ThreadLocalRandom.current().nextInt(candidates.size()));

        var original = buffered.get(index);
        var adjusted = LaplaceNoise.apply(original, grid);
        buffered.set(index, adjusted);

        double[] noise = new double[original.f3.length];
        for (int i = 0; i < noise.length; i++) {
            noise[i] = adjusted.f3[i] - original.f3[i];
        }

        System.out.printf(
                "DME noise added> user=%s, sequence=%d, recordId=%d, "
                        + "before=%s, after=%s, noise=%s%n",
                original.f0, original.f2, original.f1,
                Arrays.toString(original.f3),
                Arrays.toString(adjusted.f3),
                Arrays.toString(noise));

        return 1;
    }

//    private static final double EPSILON = 1.0;

//    private Tuple2<Double, Double> adjustCoordinates(
//            Tuple2<Double, Double> coordinates) {
//        double axisEpsilon = EPSILON / 2.0;
//        double longitudeScale = grid.longitudeSpan() / axisEpsilon;
//        double latitudeScale = grid.latitudeSpan() / axisEpsilon;
//
//        return Tuple2.of(
//                coordinates.f0 + sampleLaplace(longitudeScale),
//                coordinates.f1 + sampleLaplace(latitudeScale));
//
//        return Tuple2.of(
//                coordinates.f0,
//                coordinates.f1);
//    }
//
//    private static double sampleLaplace(double scale) {
//        ThreadLocalRandom random = ThreadLocalRandom.current();
//        double magnitude = -scale * Math.log1p(-random.nextDouble());
//        return random.nextBoolean() ? magnitude : -magnitude;
//    }

    @Override
    public void close() throws Exception {
        try {
            if (targetWriter != null) {
                targetWriter.close();
            }
        } finally {
            super.close();
        }
    }
}