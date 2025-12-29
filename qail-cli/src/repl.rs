//! REPL mode for interactive QAIL queries

use colored::*;
use qail_core::transpiler::ToSql;

/// Run the interactive REPL mode.
pub fn run_repl() {
    use rustyline::DefaultEditor;
    use rustyline::error::ReadlineError;

    println!("{}", "🪝 QAIL REPL — Interactive Mode".cyan().bold());
    println!(
        "{}",
        "Type queries to see generated SQL. Commands:".dimmed()
    );
    println!("  {}  - Exit the REPL", ".exit".yellow());
    println!("  {} - Show symbol reference", ".help".yellow());
    println!("  {} - Clear screen", ".clear".yellow());
    println!();

    let mut rl = match DefaultEditor::new() {
        Ok(editor) => editor,
        Err(e) => {
            eprintln!("{} {}", "Failed to initialize REPL:".red(), e);
            return;
        }
    };

    // Load history if available
    let history_path = dirs::home_dir()
        .map(|p| p.join(".qail_history"))
        .unwrap_or_default();
    let _ = rl.load_history(&history_path);

    loop {
        let prompt = "qail> ".cyan().bold().to_string();
        match rl.readline(&prompt) {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(line);

                match line {
                    ".exit" | ".quit" | "exit" | "quit" => {
                        println!("{}", "Goodbye! 👋".green());
                        break;
                    }
                    ".help" | "help" => {
                        show_repl_help();
                        continue;
                    }
                    ".clear" | "clear" => {
                        print!("\x1B[2J\x1B[1;1H");
                        continue;
                    }
                    ".symbols" | "symbols" => {
                        println!(
                            "{}",
                            "Type 'qail symbols' in a terminal to see the full symbol reference."
                                .dimmed()
                        );
                        continue;
                    }
                    _ => {}
                }

                match qail_core::parse(line) {
                    Ok(cmd) => {
                        let sql = cmd.to_sql();
                        println!("{} {}", "→".green(), sql.white().bold());
                        println!();
                    }
                    Err(e) => {
                        eprintln!("{} {}", "✗".red(), e.to_string().red());
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("{}", "^C".dimmed());
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("{}", "Goodbye! 👋".green());
                break;
            }
            Err(err) => {
                eprintln!("{} {:?}", "Error:".red(), err);
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);
}

/// Show REPL help information.
pub fn show_repl_help() {
    println!("{}", "QAIL REPL Commands:".cyan().bold());
    println!("  {}     - Exit the REPL", ".exit".yellow());
    println!("  {}     - Show this help", ".help".yellow());
    println!("  {}    - Clear screen", ".clear".yellow());
    println!("  {}  - Show symbol reference", ".symbols".yellow());
    println!();
    println!("{}", "Query Examples (v2 keyword syntax):".cyan().bold());
    println!("  get users fields *");
    println!("  get orders fields id, total where status = 'pending' limit 10");
    println!("  set users values verified = true where id = $1");
    println!("  add users fields name, email values 'Alice', 'alice@test.com'");
    println!("  del users where id = $1");
    println!();
}
