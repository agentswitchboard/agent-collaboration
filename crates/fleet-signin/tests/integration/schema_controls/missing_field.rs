struct StampRow {
    id: Option<String>,
    organization_statement: String,
}

fn main() {
    let _query = sqlx::query_as!(StampRow, "SELECT id FROM signin_organization_identity");
}
