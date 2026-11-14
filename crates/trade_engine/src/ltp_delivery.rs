use crate::ltp_outbox::{DeliveryStore, Fact, Identity, MAX_PENDING};
use crate::ltp_persist::{ExecutionPublisher, RolePublisher, StatementPublisher};
use anyhow::{ensure, Result};

pub struct Delivery {
    pub store: DeliveryStore,
    executions: ExecutionPublisher,
    roles: RolePublisher,
    statements: StatementPublisher,
}
impl Delivery {
    pub fn new(store: DeliveryStore) -> Result<Self> {
        Ok(Self {
            store,
            executions: ExecutionPublisher::new()?,
            roles: RolePublisher::new()?,
            statements: StatementPublisher::new()?,
        })
    }
    pub async fn poll(&mut self) -> Result<()> {
        self.store.ensure_healthy()?;
        let mut acks = self.executions.poll()?;
        acks.extend(self.roles.poll()?);
        acks.extend(self.statements.poll()?);
        self.store
            .acknowledge(acks.into_iter().map(|a| a.to_vec()).collect())
            .await?;
        for fact in self.store.state().pending.values() {
            match fact {
                Fact::Execution(r) if self.executions.has_capacity() => {
                    self.executions.enqueue(r)?
                }
                Fact::Role(r) if self.roles.has_capacity() => self.roles.enqueue(r)?,
                Fact::Statement(r) if self.statements.has_capacity() => {
                    self.statements.enqueue(r)?
                }
                _ => {}
            }
        }
        Ok(())
    }
    pub async fn save(
        &mut self,
        facts: Vec<Fact>,
        identities: Vec<Identity>,
        statement_end: Option<i64>,
    ) -> Result<()> {
        ensure!(
            facts.len() <= MAX_PENDING,
            "RapidX fact batch exceeds outbox bound"
        );
        // Leave the upstream message/batch unconsumed until durable capacity returns.
        // The receive queues and durable spool are bounded; facts are never evicted.
        while self.store.state().pending.len() + facts.len() > MAX_PENDING {
            self.poll().await?;
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        self.store.change(facts, identities, statement_end).await
    }
    pub async fn drain(&mut self, timeout: std::time::Duration) -> Result<()> {
        let until = tokio::time::Instant::now() + timeout;
        while !self.store.state().pending.is_empty() {
            ensure!(
                tokio::time::Instant::now() < until,
                "RapidX durable delivery ACK timeout; pending facts retained"
            );
            self.poll().await?;
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        Ok(())
    }
}
