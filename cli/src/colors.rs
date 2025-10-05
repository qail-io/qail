//! Lightweight ANSI terminal colors — replaces the `colored` crate.
//!
//! Usage: `"text".red()`, `"text".green().bold()`, etc.

use std::fmt;

/// A colored string wrapper that stores ANSI escape sequences.
pub struct Painted {
    pub text: String,
    pub prefix: String,
}

impl fmt::Display for Painted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.prefix.is_empty() {
            write!(f, "{}", self.text)
        } else {
            write!(f, "{}{}\x1b[0m", self.prefix, self.text)
        }
    }
}

impl Painted {
    fn new(text: String, code: &str) -> Self {
        Self {
            text,
            prefix: format!("\x1b[{code}m"),
        }
    }

    fn wrap(self, code: &str) -> Self {
        Self {
            text: self.text,
            prefix: format!("{}\x1b[{code}m", self.prefix),
        }
    }

    pub fn bold(self) -> Self { self.wrap("1") }
    pub fn dimmed(self) -> Self { self.wrap("2") }
    pub fn italic(self) -> Self { self.wrap("3") }
    pub fn underline(self) -> Self { self.wrap("4") }
    pub fn red(self) -> Self { self.wrap("31") }
    pub fn green(self) -> Self { self.wrap("32") }
    pub fn yellow(self) -> Self { self.wrap("33") }
    pub fn blue(self) -> Self { self.wrap("34") }
    pub fn magenta(self) -> Self { self.wrap("35") }
    pub fn cyan(self) -> Self { self.wrap("36") }
    pub fn white(self) -> Self { self.wrap("37") }
    #[allow(dead_code)]
    pub fn bright_black(self) -> Self { self.wrap("90") }
}

/// Trait that adds color methods to `&str` and `String`.
pub trait Colorize {
    fn paint(&self, code: &str) -> Painted;
    fn bold(&self) -> Painted { self.paint("1") }
    fn dimmed(&self) -> Painted { self.paint("2") }
    fn italic(&self) -> Painted { self.paint("3") }
    fn underline(&self) -> Painted { self.paint("4") }
    fn red(&self) -> Painted { self.paint("31") }
    fn green(&self) -> Painted { self.paint("32") }
    fn yellow(&self) -> Painted { self.paint("33") }
    fn blue(&self) -> Painted { self.paint("34") }
    fn magenta(&self) -> Painted { self.paint("35") }
    fn cyan(&self) -> Painted { self.paint("36") }
    fn white(&self) -> Painted { self.paint("37") }
    #[allow(dead_code)]
    fn bright_black(&self) -> Painted { self.paint("90") }
}

impl Colorize for &str {
    fn paint(&self, code: &str) -> Painted {
        Painted::new(self.to_string(), code)
    }
}

impl Colorize for String {
    fn paint(&self, code: &str) -> Painted {
        Painted::new(self.clone(), code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_color() {
        let s = "hello".red();
        assert_eq!(format!("{s}"), "\x1b[31mhello\x1b[0m");
    }

    #[test]
    fn test_chained() {
        let s = "hello".red().bold();
        assert_eq!(format!("{s}"), "\x1b[31m\x1b[1mhello\x1b[0m");
    }

    #[test]
    fn test_string_colorize() {
        let s = String::from("world").green();
        assert_eq!(format!("{s}"), "\x1b[32mworld\x1b[0m");
    }
}
