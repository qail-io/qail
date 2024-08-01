use crate::parser::parse;
use crate::ast::*;

#[test]
fn test_txn_commands() {
    // V2 transaction syntax
    let cmd = parse("begin").unwrap();
    assert_eq!(cmd.action, Action::TxnStart);
    
    let cmd = parse("commit").unwrap();
    assert_eq!(cmd.action, Action::TxnCommit);
    
    let cmd = parse("rollback").unwrap();
    assert_eq!(cmd.action, Action::TxnRollback);
}
