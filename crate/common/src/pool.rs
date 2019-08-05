use futures::future::{loop_fn, ok, Either, Future, Loop};
use log::error;
use std::collections::HashMap;
use std::sync::RwLock;
use tokio_postgres::{Client, NoTls, Statement};

use super::*;

const CONN_LIMIT: u64 = 480;

pub struct PgPool {
    conn_string: &'static str,
    count: RwLock<u64>,
    pool: RwLock<Vec<(Client, HashMap<&'static str, Statement>)>>,
}

impl PgPool {
    pub fn new(conn_string: &'static str) -> Self {
        Self {
            conn_string,
            count: RwLock::new(0),
            pool: RwLock::new(Vec::new()),
        }
    }

    pub fn take(&'static self) -> impl Future<Item = PgHandle, Error = UserError> {
        loop_fn((), move |_|
        //         match self.pool.try_write() {
        //     Ok(mut pool_guard) => match pool_guard.pop() {
        //         Some(client) => Ok(Async::Ready(Some(client))),
        //         None => {
        //             drop(pool_guard);
        //             match self.count.try_read() {
        //                 Ok(count_guard) => {
        //                     if *count_guard < CONN_LIMIT {
        //                         drop(count_guard);
        //                         *self.count.write().unwrap() += 1;
        //                         Ok(Async::Ready(None))
        //                     } else {
        //                         Ok(Async::NotReady)
        //                     }
        //                 }
        //                 Err(TryLockError::WouldBlock) => Ok(Async::NotReady),
        //                 Err(TryLockError::Poisoned(e)) => panic!("{}", e),
        //             }
        //         }
        //     },
        //     Err(TryLockError::WouldBlock) => Ok(Async::NotReady),
        //             Err(TryLockError::Poisoned(e)) => panic!("{}", e),

        //         }
                // }

                match self.pool.write().unwrap().pop() {
                    Some(client) => Ok(Loop::Break(Some(client))),
                    None => {
                        let count_read = self.count.read().unwrap();
                        if *count_read < CONN_LIMIT {
                            drop(count_read);
                            *self.count.write().unwrap() += 1;
                            Ok(Loop::Break(None))
                        } else {
                            Ok(Loop::Continue(()))
                        }
                    }
                })
        .and_then(move |maybe_pair| match maybe_pair {
            Some(pair) => Either::A(ok(PgHandle {
                parent: self,
                prepared: Some(pair.1),
                inner: Some(pair.0),
            })),
            None => Either::B(
                tokio_postgres::connect(self.conn_string, NoTls)
                    .map(move |(client, connection)| {
                        let connection = connection
                            .then(move |res| {
                                *self.count.write().unwrap() -= 1;
                                res
                            })
                            .map_err(|e| {
                                error!("connection error: {}", e);
                                std::process::exit(1);
                            });
                        tokio::spawn(connection);

                        PgHandle {
                            parent: self,
                            prepared: Some(HashMap::new()),
                            inner: Some(client),
                        }
                    })
                    .map_err(map_ue!()),
            ),
        })
    }

    pub fn give(&self, pair: (Client, HashMap<&'static str, Statement>)) {
        self.pool.write().unwrap().push(pair);
    }
}

pub struct PgHandle {
    parent: &'static PgPool,
    prepared: Option<HashMap<&'static str, Statement>>,
    inner: Option<Client>,
}

impl PgHandle {
    pub fn cache_prepared(
        &mut self,
        query: &'static str,
    ) -> impl Future<Item = Statement, Error = UserError> + '_ {
        let prepared = self.prepared.as_mut().unwrap();

        if let Some(statement) = prepared.get(query) {
            return Either::B(ok(statement.clone()));
        }

        Either::A(self.inner.as_mut().unwrap().prepare(query).map_err(map_ue!()).map(move |statement| {
            prepared.insert(query, statement.clone());
            statement
        }))
    }
}

impl Drop for PgHandle {
    fn drop(&mut self) {
        self.parent
            .give((self.inner.take().unwrap(), self.prepared.take().unwrap()))
    }
}

impl std::ops::Deref for PgHandle {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PgHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap()
    }
}
