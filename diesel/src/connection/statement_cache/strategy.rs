use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;

use crate::{backend::Backend, result::Error};

use super::{
    CacheSize, MaybeCached, PrepareForCache, QueryFragmentForCachedStatement, StatementCacheKey,
};

/// Implement this trait, in order to control statement caching.
#[allow(unreachable_pub)]
pub trait StatementCacheStrategy<DB, Statement>
where
    DB: Backend,
    StatementCacheKey<DB>: Hash + Eq,
{
    /// Returns which strategy is implemented by this trait
    fn strategy(&self) -> CacheSize;

    /// Every query (which is safe to cache) will go through this function
    /// Implementation will decide whether to cache statement or not
    fn get(
        &mut self,
        key: StatementCacheKey<DB>,
        backend: &DB,
        source: &dyn QueryFragmentForCachedStatement<DB>,
        prepare_fn: &mut dyn FnMut(&str, PrepareForCache) -> Result<Statement, Error>,
    ) -> Result<MaybeCached<'_, Statement>, Error>;
}

/// Cache all (safe) statements for as long as connection is alive.
#[allow(missing_debug_implementations, unreachable_pub)]
pub struct WithCacheStrategy<DB, Statement>
where
    DB: Backend,
{
    cache: HashMap<StatementCacheKey<DB>, Statement>,
}

impl<DB, Statement> Default for WithCacheStrategy<DB, Statement>
where
    DB: Backend,
{
    fn default() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}

impl<DB, Statement> StatementCacheStrategy<DB, Statement> for WithCacheStrategy<DB, Statement>
where
    DB: Backend,
    StatementCacheKey<DB>: Hash + Eq,
    DB::TypeMetadata: Clone,
    DB::QueryBuilder: Default,
{
    fn get(
        &mut self,
        key: StatementCacheKey<DB>,
        backend: &DB,
        source: &dyn QueryFragmentForCachedStatement<DB>,
        prepare_fn: &mut dyn FnMut(&str, PrepareForCache) -> Result<Statement, Error>,
    ) -> Result<MaybeCached<'_, Statement>, Error> {
        let entry = self.cache.entry(key);
        match entry {
            Entry::Occupied(e) => Ok(MaybeCached::Cached(e.into_mut())),
            Entry::Vacant(e) => {
                let sql = e.key().sql(source, backend)?;
                let st = prepare_fn(&sql, PrepareForCache::Yes)?;
                Ok(MaybeCached::Cached(e.insert(st)))
            }
        }
    }

    fn strategy(&self) -> CacheSize {
        CacheSize::Unbounded
    }
}

/// No statements will be cached,
#[allow(missing_debug_implementations, unreachable_pub)]
#[derive(Clone, Copy, Default)]
pub struct WithoutCacheStrategy {}

impl<DB, Statement> StatementCacheStrategy<DB, Statement> for WithoutCacheStrategy
where
    DB: Backend,
    StatementCacheKey<DB>: Hash + Eq,
    DB::TypeMetadata: Clone,
    DB::QueryBuilder: Default,
{
    fn get(
        &mut self,
        key: StatementCacheKey<DB>,
        backend: &DB,
        source: &dyn QueryFragmentForCachedStatement<DB>,
        prepare_fn: &mut dyn FnMut(&str, PrepareForCache) -> Result<Statement, Error>,
    ) -> Result<MaybeCached<'_, Statement>, Error> {
        let sql = key.sql(source, backend)?;
        Ok(MaybeCached::CannotCache(prepare_fn(
            &sql,
            PrepareForCache::No,
        )?))
    }

    fn strategy(&self) -> CacheSize {
        CacheSize::Disabled
    }
}

/// Utilities that help to introspect statement caching behaviour in tests.
#[allow(dead_code)]
#[cfg(test)]
pub mod testing_utils {
    use std::cell::RefCell;

    use super::*;

    thread_local! {
        static INTROSPECT_CACHING_STRATEGY: RefCell<Vec<CallInfo>> = const { RefCell::new(Vec::new()) };
    }

    /// Wraps caching strategy and records all outcome of all calls to `get`.
    /// Later all recorded calls can be observed by calling free function
    /// [`consume_statement_caching_calls`].
    #[allow(missing_debug_implementations)]
    pub struct IntrospectCachingStrategy<Backend, Statement> {
        inner: Box<dyn StatementCacheStrategy<Backend, Statement>>,
    }

    impl<DB, Statement> IntrospectCachingStrategy<DB, Statement>
    where
        DB: Backend + 'static,
        Statement: 'static,
        StatementCacheKey<DB>: Hash + Eq,
        DB::TypeMetadata: Clone,
        DB::QueryBuilder: Default,
    {
        /// Wrap internal cache strategy and record all calls to it that happen on a thread.
        /// Later call `consume_statement_caching_calls` to get results.
        pub fn new<Strategy>(strategy: Strategy) -> Self
        where
            Strategy: StatementCacheStrategy<DB, Statement> + 'static,
        {
            consume_statement_caching_calls(); // clear everything once new connection is created
            IntrospectCachingStrategy {
                inner: Box::new(strategy),
            }
        }
    }

    /// Outcome of call to [`StatementCacheStrategy::get`] implementation.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CachingOutcome {
        /// Statement was taken from cache
        UseCached,
        /// Statement was put to cache
        Cache,
        /// Statement wasn't cached
        DontCache,
    }

    /// Result summary of call to [`StatementCacheStrategy::get`]
    #[derive(Debug, PartialEq, Eq)]
    pub struct CallInfo {
        /// Sql query
        pub sql: String,
        /// Caching outcome
        pub outcome: CachingOutcome,
    }

    /// Helper type that makes it simpler to verify [`CachingOutcome`].
    #[derive(Debug)]
    pub struct IntrospectedCalls {
        /// All introspected calls
        pub calls: Vec<CallInfo>,
    }

    impl IntrospectedCalls {
        /// Count how many calls matches required outcome.
        pub fn count(&self, outcome: CachingOutcome) -> usize {
            self.calls
                .iter()
                .filter(|info| info.outcome == outcome)
                .count()
        }
        /// Returns true if there was not calls introspected.
        pub fn is_empty(&self) -> bool {
            self.calls.is_empty()
        }
    }

    /// Return all calls that was recorded for current thread using [`IntrospectCachingStrategy`]
    pub fn consume_statement_caching_calls() -> IntrospectedCalls {
        IntrospectedCalls {
            calls: INTROSPECT_CACHING_STRATEGY.with_borrow_mut(std::mem::take),
        }
    }

    impl<DB, Statement> StatementCacheStrategy<DB, Statement>
        for IntrospectCachingStrategy<DB, Statement>
    where
        DB: Backend,
        StatementCacheKey<DB>: Hash + Eq,
        DB::TypeMetadata: Clone,
        DB::QueryBuilder: Default,
    {
        fn get(
            &mut self,
            key: StatementCacheKey<DB>,
            backend: &DB,
            source: &dyn QueryFragmentForCachedStatement<DB>,
            prepare_fn: &mut dyn FnMut(&str, PrepareForCache) -> Result<Statement, Error>,
        ) -> Result<MaybeCached<'_, Statement>, Error> {
            let mut outcome = None;

            let sql = key.sql(source, backend)?.to_string();
            let res = self
                .inner
                .get(key, backend, source, &mut |sql, is_cached| {
                    outcome = Some(match is_cached {
                        PrepareForCache::Yes => CachingOutcome::Cache,
                        PrepareForCache::No => CachingOutcome::DontCache,
                    });
                    prepare_fn(sql, is_cached)
                })?;
            INTROSPECT_CACHING_STRATEGY.with_borrow_mut(|calls| {
                calls.push(CallInfo {
                    sql,
                    outcome: outcome.unwrap_or(CachingOutcome::UseCached),
                })
            });
            Ok(res)
        }

        fn strategy(&self) -> CacheSize {
            self.inner.strategy()
        }
    }
}
