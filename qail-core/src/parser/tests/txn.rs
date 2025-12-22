use crate::parser::parse;
use crate::ast::*;

#[test]
fn test_txn_commands() {
    let cmd = parse("txn::start").unwrap();
    assert_eq!(cmd.action, Action::TxnStart);
    
    let cmd = parse("txn::commit").unwrap();
    assert_eq!(cmd.action, Action::TxnCommit);
    
    let cmd = parse("txn::rollback").unwrap();
    assert_eq!(cmd.action, Action::TxnRollback);
}
