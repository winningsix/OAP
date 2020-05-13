package com.intel.oap.common.storage;

public class PMemChunk implements Chunk {
    long baseAddress;
    long offset;

    public void writeDataToStore(Object baseObj, long baseAddress, long offset){
        // TODO Platform copy to the address
    }

}
