package de.invesdwin.context.persistence.timeseriesdb.array;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum FlyweightPrimitiveArrayType {
    Boolean,
    Double,
    Long,
    Int;
}
