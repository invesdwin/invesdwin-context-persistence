package de.invesdwin.context.persistence.leveldb.serde;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.LongSerde;
import ezdb.serde.Serde;

@Immutable
public class FDateSerde implements Serde<FDate> {

    //CHECKSTYLE:OFF
    public final static FDateSerde get = new FDateSerde();

    //CHECKSTYLE:ON

    @Override
    public FDate fromBytes(final byte[] bytes) {
        final Long time = LongSerde.get.fromBytes(bytes);
        if (time == null) {
            return null;
        }
        return new FDate(time);
    }

    @Override
    public byte[] toBytes(final FDate obj) {
        if (obj == null) {
            throw new NullPointerException();
        }
        final Long time = obj.millisValue();
        return LongSerde.get.toBytes(time);
    }

}
