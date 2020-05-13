package com.intel.oap.common.storage;

import com.google.common.primitives.Bytes;

import java.util.Arrays;
import java.util.Iterator;

//FIXME should new this by parameter instead of passing in by Spark
/**
 * store memta info such as map between chunkID and physical baseAddr in pmem.
 * provide methods to get chunks iterator with logicalID provided.
 */
public abstract class PMemDataStore {
    byte[] id;
    ChunkAPI impl;
    ChunkAPI fallback;
    FileChunk fileChunk;
    MemoryStats stats;

    public PMemDataStore(byte [] id, MemoryStats stats){
        this.id = id;
        this.stats = stats;
        fileChunk = new FileChunk();
    }

    /**
     *
     * @param id logical ID
     * @return
     */
    public abstract Iterator<Chunk> getInputChunkIterator();

    /**
     * provide trunk for output stream write, need update metadata for
     * this stream, like chunkID++, totalsize, etc. need implement methods next()
     * @param id
     * @param chunkSize
     * @return
     */
    public Iterator<Chunk> getOutputChunkIterator() {
        return new Iterator<Chunk>() {
            long chuckID = 0;

            @Override
            public boolean hasNext() {
                throw new RuntimeException("Unsupported operation");
            }

            @Override
            public Chunk next() {
                chuckID++;
                byte[] physicalID = Bytes.concat(PMemDataStore.LongToBytes(chuckID), id);
                Chunk chuck = impl.getChunk(physicalID);
                if (chuck == null) {
                    return fileChunk;
                }
                return chuck;
            }
        };
    }

    private static byte[] LongToBytes(long vl) {
        //TODO
        return null;
    }

    /**
     * get metadata for this logical stream with format <Long + Int + boolean>
     * @param id logical ID
     * @return StreamMeta
     */
    public abstract StreamMeta getStreamMeta(byte[] id);

    /**
     * put metadata info in pmem? or HashMap?
     * @param id logical ID
     * @param streamMeta
     */
    public abstract void putStreamMeta(byte[] id, StreamMeta streamMeta);

}