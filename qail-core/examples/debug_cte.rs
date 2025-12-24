//! Debug Option<Uuid> NullUuid support

use qail_core::ast::Value;
use uuid::Uuid;

fn main() {
    println!("=== Debug Option<Uuid> with NullUuid ===\n");
    
    // Test 1: Option<Uuid> (Some) -> Value::Uuid
    let some_uuid: Option<Uuid> = Some(Uuid::new_v4());
    let v1: Value = some_uuid.into();
    println!("Option<Uuid> (Some) -> {:?}", v1);
    
    // Test 2: Option<Uuid> (None) -> Value::NullUuid
    let none_uuid: Option<Uuid> = None;
    let v2: Value = none_uuid.into();
    println!("Option<Uuid> (None) -> {:?}", v2);
    
    // Verify types
    match &v1 {
        Value::Uuid(_) => println!("✅ Some(Uuid) -> Value::Uuid"),
        _ => println!("❌ Some(Uuid) -> {:?} (WRONG!)", v1),
    }
    
    match &v2 {
        Value::NullUuid => println!("✅ None -> Value::NullUuid"),
        Value::Null => println!("❌ None -> Value::Null (WRONG - would bind as String NULL!)"),
        _ => println!("❌ None -> {:?} (WRONG!)", v2),
    }
}
