package io.bridgeorm.core

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * BridgeORM Kotlin Extensions.
 * Rule 4 (Meaningful Identifier): Use idiomatic Kotlin naming and coroutines.
 */
object BridgeORMClient {
    
    /**
     * Connects to the database asynchronously.
     * Rule 8 (Intent): We use Dispatchers.IO to ensure JNI calls don't block
     * the main thread or UI thread.
     */
    suspend fun connect(url: String) = withContext(Dispatchers.IO) {
        try {
            // Call the Java JNI wrapper
            io.bridgeorm.core.BridgeORM.connect(url)
        } catch (e: Exception) {
            // Rule 6 (Explicit Error Handling): Rethrow or handle based on context
            throw e
        }
    }
}
