package com.example;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.HashSet;
import java.util.Set;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class DataConsumer implements
        Sink<Tuple4<String, Integer, Long, double[]>> {

    private final RegionGrid grid;
    DataConsumer(RegionGrid grid) {
        this.grid = grid;
    }

    @Override
    public SinkWriter<Tuple4<String, Integer, Long, double[]>>
    createWriter(WriterInitContext context) throws IOException{
        return new ConsumerWriter(grid);
    }

    private static final class ConsumerWriter implements
            SinkWriter<Tuple4<String, Integer, Long, double[]>> {
        private final Evaluation evaluation = new Evaluation();
        private final Set<String> seenUsers = new HashSet<>();
        private long received;
        private final RegionGrid grid;
        private long targetCount;
        private final BufferedWriter targetWriter;
        private long matchedTargetCount;
        private double totalLatencyMs;

        ConsumerWriter(RegionGrid grid) throws IOException {
            this.grid = grid;
            targetWriter = Files.newBufferedWriter(
                    Path.of("outputs", "consumer-target.csv"));
            targetWriter.write("user,sequence");
            targetWriter.newLine();
        }

        @Override
        public void write (
                Tuple4<String, Integer, Long, double[]> record,
                Context context) throws IOException{
            received++;

            Long dmeReceivedAt = context.timestamp();
            if (dmeReceivedAt != null) {
                totalLatencyMs += System.currentTimeMillis() - dmeReceivedAt;
                matchedTargetCount++;
            }

            if (grid.classifyGroup(record.f3).contains(RegionType.TARGET)) {
                targetCount++;
                targetWriter.write("\"" + record.f0.replace("\"", "\"\"") + "\"," + record.f2);
                targetWriter.newLine();
            }

            seenUsers.add(record.f0);

//            if (received <= 5) {
//                System.out.println("Data consumer sample> " + record);
//            }
        }

        @Override
        public void flush(boolean endOfInput) {
            if (endOfInput) {
                System.out.printf(
                        "Data consumer has received %d records from %d data sources.%n",
                        received, seenUsers.size());
                if (matchedTargetCount == 0) {
                    System.out.println("Average Latency: N/A");
                } else {
                    System.out.printf(
                            "Average Latency: %.4f ms（ %d batches）%n",
                            totalLatencyMs / matchedTargetCount, matchedTargetCount);
                }
            }
            System.out.printf(
                    "Data Consumer> Received %d records, from %d data sources，%d target patterns detected.%n",
                    received, seenUsers.size(), targetCount);
        }

        @Override
        public void close() throws IOException {
            targetWriter.close();
        }
    }
}
