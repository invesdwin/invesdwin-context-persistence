package de.invesdwin.context.persistence.timeseries.serde;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.math.decimal.Decimal;
import de.invesdwin.util.math.decimal.TimedDecimal;
import de.invesdwin.util.time.date.FDate;
import ezdb.serde.Serde;
import ezdb.serde.SerializingSerde;
import ezdb.serde.TypeDelegateSerde;

@Immutable
public class ExtendedTypeDelegateSerde<O> extends TypeDelegateSerde<O> {

    public ExtendedTypeDelegateSerde(final Class<O> type) {
        super(type);
    }

    //CHECKSTYLE:OFF
    @Override
    protected Serde<?> newDelegate(final Class<O> type) {
        //CHECKSTYLE:ON
        if (Reflections.isVoid(type)) {
            return VoidSerde.GET;
        }
        if (double.class.isAssignableFrom(type) || Double.class.isAssignableFrom(type)) {
            return DoubleSerde.GET;
        }
        if (Decimal.class.isAssignableFrom(type)) {
            return DecimalSerde.GET;
        }
        if (FDate.class.isAssignableFrom(type)) {
            return FDateSerde.GET;
        }
        if (Boolean.class.isAssignableFrom(type) || boolean.class.isAssignableFrom(type)) {
            return BooleanSerde.GET;
        }
        if (Integer.class.isAssignableFrom(type) || int.class.isAssignableFrom(type)) {
            return IntegerSerde.GET;
        }
        if (TimedDecimal.class.isAssignableFrom(type)) {
            return TimedDecimalSerde.GET;
        }
        final Serde<?> serde = super.newDelegate(type);
        if (serde instanceof SerializingSerde) {
            return new RemoteFastSerializingSerde<O>(true, type);
        } else {
            return serde;
        }
    }

}
