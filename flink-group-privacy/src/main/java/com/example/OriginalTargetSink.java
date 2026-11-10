package com.example;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class OriginalTargetSink implements
        Sink<Tuple4<String, Integer, Long, double[]>> {

    private final RegionGrid grid;

    OriginalTargetSink(RegionGrid grid) {
        this.grid = grid;
    }

    @Override
    public SinkWriter<Tuple4<String, Integer, Long, double[]>>
    createWriter(WriterInitContext context) throws IOException {
        BufferedWriter writer =
                Files.newBufferedWriter(Path.of("outputs", "original-target.csv"));

        try {
            writer.write("user,sequence");
            writer.newLine();
        } catch (IOException failure) {
            try {
                writer.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }

        return new SinkWriter<Tuple4<String, Integer, Long, double[]>>() {
            @Override
            public void write(
                    Tuple4<String, Integer, Long, double[]> record,
                    Context context) throws IOException {
                if (grid.classifyGroup(record.f3).contains(RegionType.TARGET)) {
                    writer.write("\"" + record.f0.replace("\"", "\"\"") + "\"," + record.f2);
                    writer.newLine();
                }
            }

            @Override
            public void flush(boolean endOfInput) throws IOException {
                writer.flush();
            }

            @Override
            public void close() throws IOException {
                writer.close();
            }
        };
    }
}
