//! Observe a single host approval loan and reject one completed write in tests.
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use central_storage::StoreFailure;
use futures_util::{future::BoxFuture, stream::BoxStream};
use sqlx_core::{
    Either, Error,
    database::Database,
    describe::Describe,
    error::BoxDynError,
    executor::{Execute, Executor},
    sql_str::SqlStr,
};

type WriteTrace = Vec<(usize, String)>;

#[derive(Clone, Debug, Default)]
pub struct ApprovalWriteProbe(Arc<Mutex<ProbeState>>);

#[derive(Debug, Default)]
struct ProbeState {
    next: Option<Option<NonZeroUsize>>,
    trace: Option<Arc<Mutex<WriteTrace>>>,
}

impl ApprovalWriteProbe {
    pub fn arm(&self, fail_after: Option<NonZeroUsize>) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.next = Some(fail_after);
        state.trace = None;
    }

    pub fn begin_loan(&self) -> Option<ApprovalWriteLoan> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let fail_after = state.next.take()?;
        let trace = Arc::new(Mutex::new(Vec::new()));
        state.trace = Some(trace.clone());
        Some(ApprovalWriteLoan { fail_after, trace })
    }

    pub fn trace(&self) -> Option<WriteTrace> {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let trace = state.trace.as_ref()?;
        Some(
            trace
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
    }
}

#[derive(Debug)]
pub struct ApprovalWriteLoan {
    fail_after: Option<NonZeroUsize>,
    trace: Arc<Mutex<WriteTrace>>,
}

impl ApprovalWriteLoan {
    pub fn after_statement(&self, statement: &str) -> Result<(), StoreFailure> {
        let mut trace = self
            .trace
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let ordinal = trace.len() + 1;
        let label = statement
            .split_whitespace()
            .take(6)
            .collect::<Vec<_>>()
            .join(" ");
        trace.push((ordinal, label));
        if self.fail_after.is_some_and(|cut| cut.get() == ordinal) {
            return Err(StoreFailure::Unavailable);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ProbedApprovalExecutor<'loan, Inner> {
    executor: Inner,
    loan: Option<&'loan ApprovalWriteLoan>,
}

impl<'loan, Inner> ProbedApprovalExecutor<'loan, Inner> {
    pub fn new(executor: Inner, loan: Option<&'loan ApprovalWriteLoan>) -> Self {
        Self { executor, loan }
    }
}

struct LabeledQuery<Query> {
    query: Query,
    label: Arc<Mutex<Option<SqlStr>>>,
}

impl<'query, Query: Execute<'query, Db>, Db: Database> Execute<'query, Db> for LabeledQuery<Query> {
    fn sql(self) -> SqlStr {
        let sql = self.query.sql();
        *self
            .label
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sql.clone());
        sql
    }

    fn statement(&self) -> Option<&Db::Statement> {
        self.query.statement()
    }

    fn take_arguments(&mut self) -> Result<Option<Db::Arguments>, BoxDynError> {
        self.query.take_arguments()
    }

    fn persistent(&self) -> bool {
        self.query.persistent()
    }
}

impl<'connection, Inner: Executor<'connection> + 'connection> Executor<'connection>
    for ProbedApprovalExecutor<'connection, Inner>
{
    type Database = Inner::Database;

    fn execute<'out, 'query: 'out, Query>(
        self,
        query: Query,
    ) -> BoxFuture<'out, Result<<Self::Database as Database>::QueryResult, Error>>
    where
        'connection: 'out,
        Query: 'query + Execute<'query, Self::Database>,
    {
        let Some(loan) = self.loan else {
            return self.executor.execute(query);
        };
        Box::pin(async move {
            let label = Arc::new(Mutex::new(None));
            let result = self
                .executor
                .execute(LabeledQuery {
                    query,
                    label: label.clone(),
                })
                .await?;
            let label = label
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let sql = label
                .as_ref()
                .ok_or_else(|| Error::Protocol("approval test write lacked SQL".into()))?;
            loan.after_statement(sql.as_str())
                .map_err(|_| Error::Protocol("approval test write rejected".into()))?;
            Ok(result)
        })
    }

    fn fetch_many<'out, 'query: 'out, Query>(
        self,
        query: Query,
    ) -> BoxStream<
        'out,
        Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            Error,
        >,
    >
    where
        'connection: 'out,
        Query: 'query + Execute<'query, Self::Database>,
    {
        self.executor.fetch_many(query)
    }

    fn fetch_optional<'out, 'query: 'out, Query>(
        self,
        query: Query,
    ) -> BoxFuture<'out, Result<Option<<Self::Database as Database>::Row>, Error>>
    where
        'connection: 'out,
        Query: 'query + Execute<'query, Self::Database>,
    {
        self.executor.fetch_optional(query)
    }

    fn prepare_with<'out>(
        self,
        sql: SqlStr,
        parameters: &'out [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'out, Result<<Self::Database as Database>::Statement, Error>>
    where
        'connection: 'out,
    {
        self.executor.prepare_with(sql, parameters)
    }

    fn describe<'out>(self, sql: SqlStr) -> BoxFuture<'out, Result<Describe<Self::Database>, Error>>
    where
        'connection: 'out,
    {
        self.executor.describe(sql)
    }
}
