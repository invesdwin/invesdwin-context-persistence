package de.invesdwin.context.persistence.tokyocabinet;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.instrument.DynamicInstrumentationReflections;
import tokyocabinet.HDB;

// CHECKSTYLE:OFF
@NotThreadSafe
public class TCHDBEX {
    public static void main(final String[] args) {
        
        for (final String path : TokyocabinetProperties.TOKYOCABINET_LIBRARY_PATHS) {
            DynamicInstrumentationReflections.addPathToJavaLibraryPath(new File(path));
        }

        // create the object
        final HDB hdb = new HDB();

        // open the database
        if (!hdb.open("casket.tch", HDB.OWRITER | HDB.OCREAT)) {
            final int ecode = hdb.ecode();
            System.err.println("open error: " + hdb.errmsg(ecode));
        }

        // store records
        if (!hdb.put("foo", "hop") || !hdb.put("bar", "step") || !hdb.put("baz", "jump")) {
            final int ecode = hdb.ecode();
            System.err.println("put error: " + hdb.errmsg(ecode));
        }

        // retrieve records
        String value = hdb.get("foo");
        if (value != null) {
            System.out.println(value);
        } else {
            final int ecode = hdb.ecode();
            System.err.println("get error: " + hdb.errmsg(ecode));
        }

        // traverse records
        hdb.iterinit();
        String key;
        while ((key = hdb.iternext2()) != null) {
            value = hdb.get(key);
            if (value != null) {
                System.out.println(key + ":" + value);
            }
        }

        // close the database
        if (!hdb.close()) {
            final int ecode = hdb.ecode();
            System.err.println("close error: " + hdb.errmsg(ecode));
        }

    }
}