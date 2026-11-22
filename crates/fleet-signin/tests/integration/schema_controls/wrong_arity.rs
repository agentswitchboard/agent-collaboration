fn main() {
    let _query = sqlx::query!("SELECT id FROM signin_organization_identity WHERE id = ?", "id", "extra");
}
