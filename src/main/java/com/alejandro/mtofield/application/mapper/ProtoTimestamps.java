package com.alejandro.mtofield.application.mapper;

import com.google.protobuf.Timestamp;

import java.time.Instant;

/** Instant a Timestamp de protobuf y vuelta. */
public final class ProtoTimestamps {

    private ProtoTimestamps() {
    }

    public static Timestamp toProto(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

    public static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
