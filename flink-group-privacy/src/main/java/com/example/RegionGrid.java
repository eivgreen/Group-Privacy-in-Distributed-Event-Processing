package com.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

final class RegionGrid implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int SIZE = 40;
    private static final int PRIVATE_COUNT = SIZE * 40 / 100;
    private static final int TARGET_COUNT = SIZE * 40 / 100;
    private static final double OUTLIER_FRACTION = 0.01;
    private final Dimension[] dimensions;

    private RegionGrid(Dimension[] dimensions) {
        this.dimensions = dimensions;
    }

    static RegionGrid fromFiles(int maxRecords, String... filenames)
            throws IOException {
        List<List<Double>> samplesByColumn = new ArrayList<>();

        for (String filename : filenames) {
            try (BufferedReader reader = Files.newBufferedReader(Path.of(filename))) {
                String line;
                long sequence = 0;

                while (sequence < maxRecords && (line = reader.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }

                    double[] values = GpsInput.parseLine(line, ++sequence).f2;

                    for (int i = 0; i < values.length; i++) {
                        if (i == samplesByColumn.size()) {
                            samplesByColumn.add(new ArrayList<>());
                        }
                        samplesByColumn.get(i).add(values[i]);
                    }
                }
            }
        }

        if (samplesByColumn.isEmpty()) {
            throw new IllegalArgumentException("No input values found");
        }

        Dimension[] dimensions = new Dimension[samplesByColumn.size()];

        for (int i = 0; i < dimensions.length; i++) {
            List<Double> samples = samplesByColumn.get(i);
            Collections.sort(samples);

            int trim = (int) Math.floor(samples.size() * OUTLIER_FRACTION);
            double lower = samples.get(trim);
            double upper = samples.get(samples.size() - 1 - trim);

            Dimension dimension = new Dimension(i);
            dimension.include(lower);
            dimension.include(upper);
            dimensions[i] = dimension;

            System.out.printf(
                    "Outliers Removed > column=%d, rawMin=%s, rawMax=%s, "
                            + "lower=%s, upper=%s, scale=%s%n",
                    i + 3, samples.get(0), samples.get(samples.size() - 1),
                    lower, upper, (upper - lower) / SIZE);
        }

        return new RegionGrid(dimensions);
    }
    boolean isPrivateGroup(double[] values) {
        if (values.length == 0 || values.length > dimensions.length) {
            throw new IllegalArgumentException("Invalid group size: " + values.length);
        }

        for (int i = 0; i < values.length; i++) {
            if (!dimensions[i].classify(values[i]).contains(RegionType.PRIVATE)) {
                return false;
            }
        }

        return true;
    }
    boolean isWithinBounds(double[] values) {
        if (values.length == 0 || values.length > dimensions.length) {
            return false;
        }

        for (int i = 0; i < values.length; i++) {
            Dimension dimension = dimensions[i];
            if (values[i] < dimension.min || values[i] > dimension.max) {
                return false;
            }
        }

        return true;
    }

    EnumSet<RegionType> classifyGroup(double[] values) {
        if (values.length == 0 || values.length > dimensions.length) {
            throw new IllegalArgumentException("Invalid group size: " + values.length);
        }

        int targets = 0;
        int privates = 0;
        StringBuilder details = new StringBuilder();

        for (int i = 0; i < values.length; i++) {
            EnumSet<RegionType> types = dimensions[i].classify(values[i]);

            if (types.contains(RegionType.TARGET)) {
                targets++;
            }
            if (types.contains(RegionType.PRIVATE)) {
                privates++;
            }

            if (i > 0) {
                details.append(", ");
            }
            details.append("column=").append(i + 3)
                    .append(" value=").append(values[i])
                    .append(" types=").append(types);
        }

        EnumSet<RegionType> group = EnumSet.noneOf(RegionType.class);
        if (targets == values.length) {
            group.add(RegionType.TARGET);
        }
        if (privates == values.length) {
            group.add(RegionType.PRIVATE);
        } else if (privates > 0 && privates == values.length - 1) {
            group.add(RegionType.NEIGHBOR_PRIVATE);
        }
        if (group.isEmpty()) {
            group.add(RegionType.NORMAL);
        }

//        System.out.printf("Classification> %s | group=%s%n", details, group);
        return group;
    }
    double minValue(int index) {
        return dimensions[index].min;
    }

    double maxValue(int index) {
        return dimensions[index].max;
    }
    double valueSpan(int index) {
        Dimension dimension = dimensions[index];
        return (dimension.max - dimension.min) / SIZE;
    }

    private static final class Dimension implements Serializable {
        private static final long serialVersionUID = 1L;

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;

        private final boolean[] privateCells;
        private final boolean[] targetCells;

        Dimension(int index) {
            Random random = new Random(42L + index);
            privateCells = selectCells(PRIVATE_COUNT, random);
            targetCells = selectCells(TARGET_COUNT, random);
        }

        private static boolean[] selectCells(int count, Random random) {
            List<Integer> indices = new ArrayList<>(SIZE);
            for (int i = 0; i < SIZE; i++) {
                indices.add(i);
            }
            Collections.shuffle(indices, random);

            boolean[] selected = new boolean[SIZE];
            for (int i = 0; i < count; i++) {
                selected[indices.get(i)] = true;
            }
            return selected;
        }

        void include(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
        }

        EnumSet<RegionType> classify(double value) {
            int index = min == max ? 0 : (int) ((value - min) / (max - min) * SIZE);
            index = Math.max(0, Math.min(SIZE - 1, index));

            EnumSet<RegionType> types = EnumSet.noneOf(RegionType.class);
            if (privateCells[index]) {
                types.add(RegionType.PRIVATE);
            }
            if (targetCells[index]) {
                types.add(RegionType.TARGET);
            }
            if (types.isEmpty()) {
                types.add(RegionType.NORMAL);
            }
            return types;
        }
    }
}

enum RegionType {
    PRIVATE, TARGET, NORMAL, NEIGHBOR_PRIVATE
}