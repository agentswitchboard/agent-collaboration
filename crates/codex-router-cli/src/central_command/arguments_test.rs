use super::*;

const REQUEST_ID: &str = "019f0000-0000-7000-8000-00000000c101";
const OPERATION_ID: &str = "019f0000-0000-7000-8000-00000000c102";

fn parse(arguments: &[&str]) -> Result<CentralCommand, super::super::CentralCommandError> {
    CentralCommand::parse(arguments.iter().map(OsString::from).collect())
}

#[test]
fn approval_list_filters_and_read_share_the_target_and_json_arguments() {
    let output = ApprovalOutput {
        fleet: FleetTargetArguments {
            target: "fleet.json".into(),
        },
        json: true,
    };
    for (flags, filter) in [
        (&[][..], ApprovalListFilter::Waiting),
        (&["--all"][..], ApprovalListFilter::All),
    ] {
        let mut args = vec!["approvals", "list", "--target", "fleet.json", "--json"];
        args.extend_from_slice(flags);
        assert_eq!(
            parse(&args).unwrap(),
            CentralCommand::Approvals(ApprovalCommand::List {
                output: output.clone(),
                filter
            })
        );
    }
    assert_eq!(
        parse(&[
            "approvals",
            "read",
            "--target",
            "fleet.json",
            REQUEST_ID,
            "--json"
        ])
        .unwrap(),
        CentralCommand::Approvals(ApprovalCommand::Read {
            output,
            request_id: REQUEST_ID.parse().unwrap()
        })
    );
}

#[test]
fn approval_decide_accepts_exactly_one_typed_answer() {
    let base = ["approvals", "decide", "--target", "fleet.json", REQUEST_ID];
    for (flags, expected) in [
        (&["--allow"][..], ApprovalChoice::Allow),
        (&["--decline"][..], ApprovalChoice::Decline),
        (
            &["--option", "1"][..],
            ApprovalChoice::Option(OfferedOptionId::new(1).unwrap()),
        ),
    ] {
        let mut args = base.to_vec();
        args.extend_from_slice(flags);
        args.extend(["--operation", OPERATION_ID, "--json"]);
        let CentralCommand::Approvals(ApprovalCommand::Decide(parsed)) = parse(&args).unwrap()
        else {
            panic!("expected approval decide arguments");
        };
        assert_eq!(parsed.decision, expected);
        assert_eq!(parsed.operation_id, Some(OPERATION_ID.parse().unwrap()));
        assert!(parsed.output.json);
    }

    for flags in [
        &[][..],
        &["--allow", "--decline"],
        &["--option", "1", "--allow"],
        &["--option", "0"],
        &["--option", "17"],
        &["--allow", "--operation", "not-a-request-id"],
        &["--question", "yes"],
    ] {
        let mut args = base.to_vec();
        args.extend_from_slice(flags);
        assert!(parse(&args).is_err(), "accepted invalid choice {flags:?}");
    }
}
