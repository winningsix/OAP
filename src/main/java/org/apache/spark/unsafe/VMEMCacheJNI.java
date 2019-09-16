package org.apache.spark.unsafe;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JNI wrapper for the vmemcache library.
 */
public class VMEMCacheJNI {

    private static final Logger LOG = LoggerFactory.getLogger(VMEMCacheJNI.class);
    private static boolean initialized = false;
    public static final String LIBRARY_NAME = "vmemcachejni";

    static {
        LOG.info("Trying to load the native library from jni...");
        NativeLoader.loadLibrary(LIBRARY_NAME);
    }

    public static synchronized int initialize(String path, long maxSize) {
        if (!initialized) {
            int success = init(path, maxSize);
            if (success == 0) {
                initialized = true;
            }
            return success;
        }
        return 0;
    }

    static native int init(String path, long maxSize);

    /* returns the number of bytes put */
    public static native int put(byte[] keyArray, ByteBuffer keyBuffer, int keyOff, int keyLen,
                                 byte[] valueArray, ByteBuffer valueBuffer,
                                 int valueOff, int valueLen);

    /* returns the number of bytes get */
    public static native int get(byte[] keyArray, ByteBuffer keyBuffer, int keyOff, int keyLen,
                                 byte[] valueArray, ByteBuffer valueBuffer,
                                 int valueOff, int maxValueLen);

    public static native int evict(byte[] keyArray, ByteBuffer keyBuffer, int keyOff, int keyLen);
}
