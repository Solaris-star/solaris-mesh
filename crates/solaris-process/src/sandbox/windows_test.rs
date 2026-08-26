use super::*;

#[test]
fn start_confirmation_waits_for_every_valid_prefix() {
    for marker in [b"".as_slice(), b"f", b"fu", b"ful", b"full"] {
        assert_eq!(classify_start_confirmation(marker), StartConfirmation::Pending);
    }
}

#[test]
fn start_confirmation_accepts_only_the_complete_marker() {
    assert_eq!(classify_start_confirmation(b"full\n"), StartConfirmation::Confirmed);
    for marker in [b"partial".as_slice(), b"full\r\n", b"full\nextra"] {
        assert_eq!(classify_start_confirmation(marker), StartConfirmation::Invalid);
    }
}
