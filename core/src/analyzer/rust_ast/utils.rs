
/// Convert snake_case to PascalCase for type names.
/// 
/// # Example
/// ```
/// assert_eq!(to_pascal_case("payment_events"), "PaymentEvents");
/// ```
pub fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().chain(chars).collect(),
                None => String::new(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_pascal_case() {
        assert_eq!(to_pascal_case("payment_events"), "PaymentEvents");
        assert_eq!(to_pascal_case("users"), "Users");
        assert_eq!(to_pascal_case("whatsapp_messages"), "WhatsappMessages");
    }
}
