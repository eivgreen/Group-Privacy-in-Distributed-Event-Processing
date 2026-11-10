package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import java.io.IOException;

final class GpsInput {
    private GpsInput() {}

    static final TypeInformation<Tuple4<String, Integer, Long, double[]>> GROUP_TYPE =
            Types.TUPLE(Types.STRING, Types.INT, Types.LONG,
                    TypeInformation.of(double[].class));

    static DataStream<Tuple4<String, Integer, Long, double[]>> readUser(
            StreamExecutionEnvironment env, String user, String filename, int maxRecords) {
        Path path = new Path(java.nio.file.Path.of(filename).toUri());
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new LimitedLineFormat(maxRecords), path)
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "GPS " + user)
                .filter(line -> !line.isBlank())
                .map(new MapFunction<String, Tuple4<String, Integer, Long, double[]>>() {
                    private long order;

                    @Override
                    public Tuple4<String, Integer, Long, double[]> map(String line) {
                        var parsed = parseLine(line, ++order);
                        return Tuple4.of(user, parsed.f0, parsed.f1, parsed.f2);
                    }
                })
                .returns(GROUP_TYPE);
    }

    static Tuple3<Integer, Long, double[]> parseLine(String line, long sequence) {
        String[] fields = line.split(",", -1);
        if (fields.length < 3) {
            throw new IllegalArgumentException(
                    "Expected an ID, a metadata column and at least one value: " + line);
        }

        int recordId = Integer.parseInt(fields[0].trim());
        double[] values = new double[fields.length - 2];

        for (int i = 0; i < values.length; i++) {
            double value = Double.parseDouble(fields[i + 2].trim());
            if (!Double.isFinite(value)) {
                throw new IllegalArgumentException(
                        "Non-finite value at column " + (i + 3) + ": " + line);
            }
            values[i] = value;
        }

        return Tuple3.of(recordId, sequence, values);
    }

    private static final class LimitedLineFormat extends SimpleStreamFormat<String> {
        private final int maxRecords;

        LimitedLineFormat(int maxRecords) {
            if (maxRecords < 1) {
                throw new IllegalArgumentException("maxRecords must be at least 1");
            }
            this.maxRecords = maxRecords;
        }

        @Override
        public Reader<String> createReader(Configuration config, FSDataInputStream stream)
                throws IOException {
            Reader<String> lines = new TextLineInputFormat().createReader(config, stream);

            return new Reader<String>() {
                private int recordsRead;

                @Override
                public String read() throws IOException {
                    if (recordsRead >= maxRecords) {
                        return null;
                    }

                    String line;
                    while ((line = lines.read()) != null) {
                        if (!line.isBlank()) {
                            recordsRead++;
                            return line;
                        }
                    }

                    return null;
                }

                @Override
                public void close() throws IOException {
                    lines.close();
                }
            };
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
