use super::*;

#[test]
fn configured_limit_is_respected() {
    let p = ResourcePolicy::from_system(Some(2));
    assert_eq!(p.max_active(), 2);
}

#[test]
fn tracks_load_and_release() {
    let mut p = ResourcePolicy::new(3);
    assert_eq!(p.available_slots(), 3);
    assert!(p.try_acquire());
    assert_eq!(p.active(), 1);
    p.release();
    assert_eq!(p.active(), 0);
}

#[test]
fn new_policy_is_not_fixed_at_five() {
    let configured = ResourcePolicy::from_system(Some(500));
    assert_eq!(configured.max_active(), 500);
    let mut p = ResourcePolicy::new(10);
    for _ in 0..10 {
        assert!(p.try_acquire());
    }
    assert!(!p.try_acquire());
}
