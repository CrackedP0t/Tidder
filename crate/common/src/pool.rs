use super::*;
use future::poll_fn;
use futures::task::Poll;
use log::error;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::RwLock;
use tokio_postgres::{Client, NoTls, Statement};

const CONN_LIMIT: u64 = 2048;

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

    pub async fn take(&'static self) -> Result<PgHandle, UserError> {
        let maybe_pair = poll_fn(move |c| match self.pool.write().unwrap().pop() {
            Some(pair) => Poll::Ready(Some(pair)),
            None => {
                let count_read = self.count.read().unwrap();
                if *count_read < CONN_LIMIT {
                    drop(count_read);
                    *self.count.write().unwrap() += 1;
                    Poll::Ready(None)
                } else {
                    c.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        })
        .await;

        match maybe_pair {
            Some(pair) => Ok(PgHandle {
                parent: self,
                prepared: Some(pair.1),
                inner: Some(pair.0),
            }),
            None => {
                let (client, connection) = tokio_postgres::connect(self.conn_string, NoTls).await?;
                let connection = connection.then(move |res| {
                    async move {
                        *self.count.write().unwrap() -= 1;
                        if let Err(e) = res {
                            error!("connection error: {}", e);
                            std::process::exit(1);
                        }
                    }
                });
                tokio::spawn(connection);

                Ok(PgHandle {
                    parent: self,
                    prepared: Some(HashMap::new()),
                    inner: Some(client),
                })
            }
        }
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
    pub async fn cache_prepare(&mut self, query: &'static str) -> Result<Statement, UserError> {
        Ok(match self.prepared.as_mut().unwrap().entry(query) {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => v
                .insert(self.inner.as_mut().unwrap().prepare(query).await?)
                .clone(),
        })
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
