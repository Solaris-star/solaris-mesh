use std::fmt;
use std::io;
use std::sync::Arc;

use crate::{ManagedChild, ProcessLaunchPolicy};

/// One synchronous process creation attempt using the policy selected by its
/// final authorizer.
pub type ProcessSpawn = Box<dyn FnOnce(ProcessLaunchPolicy) -> io::Result<ManagedChild> + Send + 'static>;

/// Performs the last authorization check immediately around process creation.
///
/// Implementations must call `spawn` at most once and retain any authorization
/// guards until that call returns.
///
/// # Internal API
///
/// This public trait exists to connect `solaris-agent` to `solaris-process`
/// across a crate boundary. It is not a stable third-party extension point or
/// an authorization boundary by itself. The owning Agent runtime must issue
/// the authorizer only after approving and binding the complete effect.
pub trait ProcessSpawnAuthorizer: Send + Sync {
    fn authorize_and_spawn(&self, spawn: ProcessSpawn) -> io::Result<ManagedChild>;
}

/// Cloneable handle to an effect-scoped final process authorizer.
///
/// This is internal cross-crate wiring and may change without providing a
/// caller-facing compatibility contract. Constructing a handle does not grant
/// Agent permission; only a handle issued by the owning Agent runtime after
/// effect approval is a valid permission credential.
#[derive(Clone)]
pub struct ProcessSpawnAuthorization {
    authorizer: Arc<dyn ProcessSpawnAuthorizer>,
}

impl ProcessSpawnAuthorization {
    /// Wraps an Agent-issued authorizer for reuse by one prepared effect
    /// execution.
    ///
    /// This constructor does not validate or grant Agent permissions. It is
    /// public only because the Agent and process runner live in separate
    /// crates.
    pub fn new(authorizer: Arc<dyn ProcessSpawnAuthorizer>) -> Self {
        Self { authorizer }
    }

    pub(crate) fn authorize_and_spawn(&self, spawn: ProcessSpawn) -> io::Result<ManagedChild> {
        self.authorizer.authorize_and_spawn(spawn)
    }
}

impl fmt::Debug for ProcessSpawnAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessSpawnAuthorization")
            .finish_non_exhaustive()
    }
}
