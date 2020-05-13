package com.intel.oap.common.storage;

import java.util.Iterator;

@Deprecated
public class SPDKPMemDataStoreImpl extends PMemDataStore {
    public SPDKPMemDataStoreImpl(byte[] id, MemoryStats stats) {
        super(id, stats);
    }

    @Override
    public Iterator<Chunk> getInputChunkIterator() {
        throw new RuntimeException("Unsupported operation");
    }

    @Override
    public Iterator<Chunk> getOutputChunkIterator() {
        return null;
    }

    @Override
    public StreamMeta getStreamMeta(byte[] id) {
        throw new RuntimeException("Unsupported operation");
    }

    @Override
    public void putStreamMeta(byte[] id, StreamMeta streamMeta) {
        throw new RuntimeException("Unsupported operation");
    }
}
