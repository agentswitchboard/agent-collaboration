struct StampRow {
    id: Option<String>,
    organization_statement: i64,
}

fn main() {
    let _query = sqlx::query_as!(StampRow, "SELECT id, organization_statement FROM signin_organization_identity");
}
