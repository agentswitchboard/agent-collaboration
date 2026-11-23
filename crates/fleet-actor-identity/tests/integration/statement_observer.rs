//! Record the SQL and encoded arguments that reach the native SQLx executor.
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use futures_util::{future::BoxFuture, stream::BoxStream};
use sqlx::{
    Either, Execute, Executor, Sqlite, SqliteConnection,
    sqlite::{SqliteArguments, SqliteQueryResult, SqliteRow, SqliteStatement, SqliteTypeInfo},
};
use sqlx_core::{describe::Describe, error::BoxDynError, sql_str::SqlStr};

#[derive(Debug)]
pub struct ObservedRead {
    pub sql: String,
    pub arguments: Option<SqliteArguments>,
}

pub type ObservedReads = Mutex<Vec<ObservedRead>>;

#[derive(Debug)]
pub struct StatementObserver<'a> {
    pub connection: &'a mut SqliteConnection,
    pub reads: Arc<ObservedReads>,
}

struct ObservedQuery<Query> {
    query: Query,
    reads: Arc<ObservedReads>,
    arguments: Option<SqliteArguments>,
}

impl<'q, Query: Execute<'q, Sqlite>> Execute<'q, Sqlite> for ObservedQuery<Query> {
    fn sql(self) -> SqlStr {
        let sql = self.query.sql();
        self.reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ObservedRead {
                sql: sql.as_str().to_owned(),
                arguments: self.arguments,
            });
        sql
    }

    fn statement(&self) -> Option<&SqliteStatement> {
        self.query.statement()
    }

    fn take_arguments(&mut self) -> Result<Option<SqliteArguments>, BoxDynError> {
        let arguments = self.query.take_arguments()?;
        self.arguments.clone_from(&arguments);
        Ok(arguments)
    }

    fn persistent(&self) -> bool {
        self.query.persistent()
    }
}

impl<'c> Executor<'c> for StatementObserver<'c> {
    type Database = Sqlite;

    fn fetch_many<'e, 'q: 'e, Query>(
        self,
        query: Query,
    ) -> BoxStream<'e, Result<Either<SqliteQueryResult, SqliteRow>, sqlx::Error>>
    where
        'c: 'e,
        Query: 'q + Execute<'q, Sqlite>,
    {
        self.connection.fetch_many(ObservedQuery {
            query,
            reads: self.reads,
            arguments: None,
        })
    }

    fn fetch_optional<'e, 'q: 'e, Query>(
        self,
        query: Query,
    ) -> BoxFuture<'e, Result<Option<SqliteRow>, sqlx::Error>>
    where
        'c: 'e,
        Query: 'q + Execute<'q, Sqlite>,
    {
        self.connection.fetch_optional(ObservedQuery {
            query,
            reads: self.reads,
            arguments: None,
        })
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [SqliteTypeInfo],
    ) -> BoxFuture<'e, Result<SqliteStatement, sqlx::Error>>
    where
        'c: 'e,
    {
        self.connection.prepare_with(sql, parameters)
    }

    fn describe<'e>(self, sql: SqlStr) -> BoxFuture<'e, Result<Describe<Sqlite>, sqlx::Error>>
    where
        'c: 'e,
    {
        self.connection.describe(sql)
    }
}
