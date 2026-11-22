use super::*;
use workspace_ids::SessionRef;

#[test]
fn the_system_mail_insert_fits_its_owning_central_schema() -> Result<(), Box<dyn std::error::Error>>
{
    let now = "2030-01-01T00:00:00Z".parse()?;
    let message = mail::MessageRecord {
        message_id: "019f0000-0000-7000-8000-000000000031".parse()?,
        sender: mail::MessageSender::Central,
        target: SessionRef {
            machine_id: "019f0000-0000-7000-8000-000000000032".parse()?,
            session_id: "schema-witness".to_owned().try_into()?,
        },
        body: mail::MessageBody::Notice { items: vec![] },
        state: mail::MessageState::Queued,
        source: None,
        created_at: now,
        updated_at: now,
    };
    macro_rules! checked_insert {
        ($file:literal, $($arg:expr),+ $(,)?) => {
            sqlx::query_file!($file, $($arg),+)
        };
    }
    let _ = system_mail_insert!(checked_insert, &message);
    Ok(())
}
