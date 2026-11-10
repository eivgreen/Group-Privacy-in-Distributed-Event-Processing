package com.example;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class NetworkDelay<T> extends RichAsyncFunction<T, T>
        implements ResultTypeQueryable<T> {

    private static final long TIMEOUT_MS = 30_000;
    private static final int CAPACITY = 1_024;

    private final TypeInformation<T> type;
    private transient ScheduledExecutorService scheduler;

    @FunctionalInterface
    interface DelaySelector<T> extends Serializable {
        long delayMillis(T record);
    }

    private final DelaySelector<T> delaySelector;

    private NetworkDelay(DelaySelector<T> delaySelector, TypeInformation<T> type) {
        this.delaySelector = delaySelector;
        this.type = type;
    }

    static <T> SingleOutputStreamOperator<T> apply(
            DataStream<T> input, long delayMs) {
        if (delayMs < 0 || delayMs >= TIMEOUT_MS) {
            throw new IllegalArgumentException("Invalid network delay: " + delayMs);
        }
        return applyVariable(input, record -> delayMs);
    }

    static <T> SingleOutputStreamOperator<T> applyVariable(
            DataStream<T> input, DelaySelector<T> delaySelector) {
        return AsyncDataStream.orderedWait(
                input,
                new NetworkDelay<>(delaySelector, input.getType()),
                TIMEOUT_MS,
                TimeUnit.MILLISECONDS,
                CAPACITY);
    }

    @Override
    public void open(OpenContext context) {
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void asyncInvoke(T record, ResultFuture<T> result) {
        long delayMs = delaySelector.delayMillis(record);

        if (delayMs < 0 || delayMs >= TIMEOUT_MS) {
            result.completeExceptionally(
                    new IllegalArgumentException("Invalid network delay: " + delayMs));
            return;
        }

        if (delayMs == 0) {
            result.complete(Collections.singletonList(record));
            return;
        }

        scheduler.schedule(
                () -> result.complete(Collections.singletonList(record)),
                delayMs,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return type;
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
