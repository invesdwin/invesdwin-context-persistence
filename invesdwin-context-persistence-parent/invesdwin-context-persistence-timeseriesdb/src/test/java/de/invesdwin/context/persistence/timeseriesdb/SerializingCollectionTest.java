package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.description.TextDescription;

@NotThreadSafe
public class SerializingCollectionTest extends ATest {

    @Test
    public void testSymlinks() throws IOException {
        final File file = new File(ContextProperties.TEMP_DIRECTORY, "testSymlinks.bin.lz4");
        final SerializingCollection<String> writer = new SerializingCollection<>(
                new TextDescription("%s", SerializingCollectionTest.class.getSimpleName()), file, false);
        for (int i = 0; i < 100; i++) {
            writer.add("asdf" + i);
        }
        writer.close();
        final File symlink = new File(ContextProperties.TEMP_DIRECTORY, file.getName() + "_symlink");
        Files.createSymbolicLink(symlink.getAbsoluteFile().toPath(), file.getAbsoluteFile().toPath());
        final SerializingCollection<String> reader = new SerializingCollection<>(
                new TextDescription("%s", SerializingCollectionTest.class.getSimpleName()), symlink, true);
        final ICloseableIterator<String> iterator = reader.iterator();
        for (int i = 0; i < 100; i++) {
            Assertions.checkEquals("asdf" + i, iterator.next());
        }
        iterator.close();
        reader.close();
    }

}
