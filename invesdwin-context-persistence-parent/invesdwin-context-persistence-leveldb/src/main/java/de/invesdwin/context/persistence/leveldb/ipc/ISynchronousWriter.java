package de.invesdwin.context.persistence.leveldb.ipc;

public interface ISynchronousWriter extends ISynchronousChannel {

    void write(int type, byte[] message);

}
