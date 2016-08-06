package de.invesdwin.context.persistence.jpa.datanucleus.internal;

import java.lang.instrument.Instrumentation;

import javax.annotation.concurrent.NotThreadSafe;

import org.datanucleus.enhancer.DataNucleusClassFileTransformer;

import de.invesdwin.context.beans.hook.IInstrumentationHook;

@NotThreadSafe
public class DatanucleusEnhancerInstrumentationHook implements IInstrumentationHook {

    @Override
    public void instrument(final Instrumentation instrumentation) {
        DataNucleusClassFileTransformer.premain("-api=JPA", instrumentation);
    }

}
