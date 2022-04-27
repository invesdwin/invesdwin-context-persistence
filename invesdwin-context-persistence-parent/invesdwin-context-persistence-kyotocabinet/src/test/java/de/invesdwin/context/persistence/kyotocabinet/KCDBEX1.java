package de.invesdwin.context.persistence.kyotocabinet;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.instrument.DynamicInstrumentationReflections;
import kyotocabinet.Cursor;
import kyotocabinet.DB;

// CHECKSTYLE:OFF
@NotThreadSafe
public class KCDBEX1 {
    public static void main(final String[] args) {

        for (final String path : KyotocabinetProperties.KYOTOCABINET_LIBRARY_PATHS) {
            DynamicInstrumentationReflections.addPathToJavaLibraryPath(new File(path));
        }

        // create the object
        final DB db = new DB();

        // open the database
        if (!db.open("casket.kch", DB.OWRITER | DB.OCREATE)) {
            System.err.println("open error: " + db.error());
        }

        // store records
        if (!db.set("foo", "hop") || !db.set("bar", "step") || !db.set("baz", "jump")) {
            System.err.println("set error: " + db.error());
        }

        // retrieve records
        final String value = db.get("foo");
        if (value != null) {
            System.out.println(value);
        } else {
            System.err.println("set error: " + db.error());
        }

        // traverse records
        final Cursor cur = db.cursor();
        cur.jump();
        String[] rec;
        while ((rec = cur.get_str(true)) != null) {
            System.out.println(rec[0] + ":" + rec[1]);
        }
        cur.disable();

        // close the database
        if (!db.close()) {
            System.err.println("close error: " + db.error());
        }

    }
}
