use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::jstring;
use crate::engine;
use std::sync::Once;
use tokio::runtime::Runtime;
use once_cell::sync::Lazy;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    Runtime::new().expect("Failed to create Tokio runtime for Java")
});

/// Rule 1 (Self-Documenting): JNI exports are clearly named to match the Java package structure.
/// This example assumes a package: io.bridgeorm.core.BridgeORM
#[no_mangle]
pub extern "system" fn Java_io_bridgeorm_core_BridgeORM_connectNative(
    mut env: JNIEnv,
    _class: JClass,
    url: JString,
) -> jstring {
    let url_str: String = env.get_string(&url).expect("Invalid URL string from Java").into();
    
    // Rule 8 (Intent): We use a shared global Tokio runtime to avoid overhead in JNI calls.
    let result = RUNTIME.block_on(async {
        engine::db::connect(&url_str).await
    });

    match result {
        Ok(_) => {
            let output = env.new_string("SUCCESS").expect("Couldn't create java string!");
            output.into_raw()
        }
        Err(e) => {
            let err_msg = format!("ERROR: {}", e);
            let output = env.new_string(err_msg).expect("Couldn't create java string!");
            output.into_raw()
        }
    }
}

/// Rule 2 (Single Responsibility): Extracts common JNI string conversion logic.
fn to_java_string(env: &mut JNIEnv, s: &str) -> jstring {
    env.new_string(s).expect("Failed to create Java string").into_raw()
}
