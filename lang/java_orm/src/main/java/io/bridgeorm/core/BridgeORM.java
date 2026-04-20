package io.bridgeorm.core;

/**
 * BridgeORM Java Client.
 * Rule 1 (Self-Documenting): The class provides high-level static methods
 * that hide the underlying JNI complexity from the developer.
 */
public class BridgeORM {
    static {
        // Rule 8 (Intent): We load the shared library compiled by Cargo.
        // The library name is defined in Cargo.toml [lib] section.
        System.loadLibrary("bridge_orm_rs");
    }

    /**
     * Connects to a database using a URL.
     * Rule 6 (Explicit Error Handling): The native method returns "SUCCESS" 
     * or an error message starting with "ERROR:".
     */
    public static void connect(String url) throws Exception {
        String result = connectNative(url);
        if (result.startsWith("ERROR:")) {
            throw new Exception("BridgeORM Connection Failed: " + result.substring(6));
        }
    }

    private static native String connectNative(String url);

    // Placeholder for other methods like query, insert, etc.
}
