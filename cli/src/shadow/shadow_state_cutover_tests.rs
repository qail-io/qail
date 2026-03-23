use super::is_current_shadow_diff_cmds_wire;

#[test]
fn detects_current_shadow_wire_payload() {
    assert!(is_current_shadow_diff_cmds_wire("QAIL-CMDS/1\n0\n"));
    assert!(!is_current_shadow_diff_cmds_wire("[]"));
    assert!(!is_current_shadow_diff_cmds_wire("{\"legacy\":true}"));
}
