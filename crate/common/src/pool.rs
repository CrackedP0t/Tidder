use futures::future::{ok, loop_fn, Loop, Either, Future};
use log::error;
use std::sync::RwLock;
use tokio_postgres::{Client, NoTls};

use super::*;

const CONN_LIMIT: u64 = 480;

pub struct PgPool {
    conn_string: &'static str,
    count: RwLock<u64>,
    pool: RwLock<Vec<Client>>,
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
                        if *self.count.read().unwrap() < CONN_LIMIT {
                            *self.count.write().unwrap() += 1;
                            Ok(Loop::Break(None))
                        } else {
                            Ok(Loop::Continue(()))
                        }
                    }
                })
        .and_then(move |maybe_client| match maybe_client {
            Some(client) => Either::A(ok(PgHandle {
                parent: self,
                inner: Some(client),
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
                            inner: Some(client),
                        }
                    })
                    .map_err(map_ue!()),
            ),
        })
    }

    pub fn give(&self, client: Client) {
        self.pool.write().unwrap().push(client);
    }
}

pub struct PgHandle {
    parent: &'static PgPool,
    inner: Option<Client>,
}

impl Drop for PgHandle {
    fn drop(&mut self) {
        self.parent.give(self.inner.take().unwrap())
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
