package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

@FunctionalInterface
public interface IFileBufferSource {

    InputStream getSource(File file) throws IOException;

}