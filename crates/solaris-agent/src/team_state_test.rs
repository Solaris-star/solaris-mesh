use super::*;

#[test]
fn facts_replace_by_team_and_key() {
    let store = TeamStateStore::default();
    let team = TeamId::from("team");
    for value in [1, 2] {
        store.set_fact(TeamFact {
            run_id: RunId::from("run"),
            team_id: team.clone(),
            key: "answer".into(),
            value: Value::from(value),
            updated_by: AgentId::from("agent"),
            updated_at_unix_ms: value,
        });
    }
    let facts = store.facts(&team);
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].value, Value::from(2));
}
