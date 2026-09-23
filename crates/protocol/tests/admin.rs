use pgsteward_protocol::admin::{AdminCommand, AdminError, TenantSettings, parse};
use pgsteward_protocol::backend::{Severity, sqlstate};

fn command(sql: &str) -> AdminCommand {
    parse(sql).unwrap_or_else(|error| panic!("`{sql}` is a command of the console: {error}"))
}

fn refusal(sql: &str) -> AdminError {
    parse(sql).expect_err(&format!("`{sql}` is not a command of the console"))
}

fn tenant(min: Option<u32>, max: Option<u32>, weight: Option<u32>) -> TenantSettings {
    TenantSettings { min, max, weight }
}

#[test]
fn every_command_the_console_has_parses() {
    let expected = [
        ("SHOW POOLS", AdminCommand::ShowPools),
        ("SHOW BUDGET", AdminCommand::ShowBudget),
        ("SHOW CLIENTS", AdminCommand::ShowClients),
        ("SHOW SERVERS", AdminCommand::ShowServers),
        ("SHOW INSTANCES", AdminCommand::ShowInstances),
        ("SHOW CONFIG", AdminCommand::ShowConfig),
        ("RELOAD", AdminCommand::Reload),
        ("PAUSE", AdminCommand::Pause),
        ("RESUME", AdminCommand::Resume),
        (
            "SET INSTANCE db-primary.internal margin = 15",
            AdminCommand::SetInstance {
                instance: "db-primary.internal".to_owned(),
                margin: 15,
            },
        ),
        (
            "SET TENANT app_web min = 30, max = 60, weight = 2",
            AdminCommand::SetTenant {
                tenant: "app_web".to_owned(),
                settings: tenant(Some(30), Some(60), Some(2)),
            },
        ),
    ];
    for (sql, want) in expected {
        assert_eq!(command(sql), want, "{sql}");
    }
}

#[test]
fn keywords_are_case_insensitive_and_the_spacing_is_free() {
    for sql in [
        "show pools",
        "ShOw   PoOlS",
        "\tSHOW\n\nPOOLS  ",
        "SHOW POOLS;",
        "  show pools ;  ",
    ] {
        assert_eq!(command(sql), AdminCommand::ShowPools, "{sql}");
    }
    assert_eq!(
        command("set tenant app_web MIN=30,Weight = 2"),
        AdminCommand::SetTenant {
            tenant: "app_web".to_owned(),
            settings: tenant(Some(30), None, Some(2)),
        }
    );
}

#[test]
fn set_tenant_takes_any_non_empty_subset_of_its_settings_in_any_order() {
    let expected = [
        ("SET TENANT t min = 1", tenant(Some(1), None, None)),
        ("SET TENANT t max = 2", tenant(None, Some(2), None)),
        ("SET TENANT t weight = 3", tenant(None, None, Some(3))),
        (
            "SET TENANT t weight = 3, min = 1",
            tenant(Some(1), None, Some(3)),
        ),
        (
            "SET TENANT t max = 2, weight = 3, min = 1",
            tenant(Some(1), Some(2), Some(3)),
        ),
    ];
    for (sql, settings) in expected {
        assert_eq!(
            command(sql),
            AdminCommand::SetTenant {
                tenant: "t".to_owned(),
                settings,
            },
            "{sql}"
        );
    }
}

#[test]
fn set_tenant_refuses_a_text_that_sets_nothing_or_sets_one_setting_twice() {
    for sql in [
        "SET TENANT app_web",
        "SET TENANT app_web min = 1, min = 2",
        "SET TENANT app_web min = 1, weight = 2, min = 1",
    ] {
        let _ = refusal(sql);
    }
}

#[test]
fn set_refuses_a_setting_the_command_does_not_have() {
    for sql in [
        "SET TENANT app_web instances = 1",
        "SET TENANT app_web budget = 100",
        "SET INSTANCE db min = 1",
        "SET INSTANCE db margin = 15, weight = 2",
        "SET BUDGET db = 100",
    ] {
        let _ = refusal(sql);
    }
}

#[test]
fn a_quoted_name_keeps_its_case_and_may_hold_what_a_word_cannot() {
    assert_eq!(
        command("SET TENANT \"App Web\" min = 1"),
        AdminCommand::SetTenant {
            tenant: "App Web".to_owned(),
            settings: tenant(Some(1), None, None),
        }
    );
    assert_eq!(
        command("SET TENANT \"a;b\" min = 1"),
        AdminCommand::SetTenant {
            tenant: "a;b".to_owned(),
            settings: tenant(Some(1), None, None),
        }
    );
    assert_eq!(
        command("SET TENANT \"say \"\"hi\"\"\" min = 1"),
        AdminCommand::SetTenant {
            tenant: "say \"hi\"".to_owned(),
            settings: tenant(Some(1), None, None),
        }
    );
    assert_eq!(
        command("SET INSTANCE \"DB-Primary\" margin = 15"),
        AdminCommand::SetInstance {
            instance: "DB-Primary".to_owned(),
            margin: 15,
        }
    );
}

#[test]
fn a_quoted_word_is_a_name_and_never_a_keyword() {
    let _ = refusal("\"SHOW\" POOLS");
    let _ = refusal("SHOW \"POOLS\"");
}

#[test]
fn a_show_the_console_does_not_have_is_refused_with_the_ones_it_has() {
    let response = refusal("SHOW DATABASES").response();
    assert_eq!(response.severity, Severity::Error);
    assert_eq!(response.code, sqlstate::SYNTAX_ERROR);
    let hint = response.hint.expect("the refusal says what it does have");
    for command in [
        "SHOW POOLS",
        "SHOW BUDGET",
        "SHOW CLIENTS",
        "SHOW SERVERS",
        "SHOW INSTANCES",
        "SHOW CONFIG",
        "RELOAD",
        "PAUSE",
        "RESUME",
        "SET INSTANCE",
        "SET TENANT",
    ] {
        assert!(hint.contains(command), "the hint leaves out {command}");
    }
}

#[test]
fn a_text_that_is_not_a_console_command_is_refused() {
    for sql in [
        "SELECT 1",
        "BEGIN",
        "SHOW",
        "SHOW POOLS EXTRA",
        "RELOAD CONFIG",
        "PAUSE app_web",
    ] {
        let refusal = refusal(sql);
        assert_eq!(refusal.response().code, sqlstate::SYNTAX_ERROR, "{sql}");
    }
}

#[test]
fn two_statements_in_one_text_are_refused() {
    for sql in [
        "SHOW POOLS; SHOW BUDGET",
        "SHOW POOLS; SHOW POOLS",
        "RELOAD; PAUSE",
    ] {
        let _ = refusal(sql);
    }
}

#[test]
fn an_empty_statement_beside_the_command_changes_nothing() {
    for sql in ["SHOW POOLS;;", "; SHOW POOLS", ";SHOW POOLS ; ;"] {
        assert_eq!(command(sql), AdminCommand::ShowPools, "{sql}");
    }
}

#[test]
fn a_text_that_holds_no_statement_is_no_command() {
    for sql in ["", "   ", "\n\t ", ";", " ; ", ";;"] {
        assert_eq!(command(sql), AdminCommand::Empty, "{sql:?}");
    }
}

#[test]
fn a_setting_that_is_not_a_count_is_refused() {
    for sql in [
        "SET INSTANCE db margin = x",
        "SET INSTANCE db margin = -1",
        "SET INSTANCE db margin = 4294967296",
        "SET INSTANCE db margin =",
        "SET INSTANCE db margin 15",
        "SET TENANT t min = 1.5",
    ] {
        let refusal = refusal(sql);
        assert_eq!(refusal.response().code, sqlstate::SYNTAX_ERROR, "{sql}");
    }
}

#[test]
fn a_refusal_says_what_the_command_it_names_expects() {
    let response = refusal("SET TENANT app_web min = 1, min = 2").response();
    assert!(
        response.message.contains("SET TENANT"),
        "the refusal names the command: {}",
        response.message
    );
}
