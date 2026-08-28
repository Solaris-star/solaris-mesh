macro_rules! forward_compare_and_append {
    () => {
        fn logical_append_capability(&self) -> $crate::runtime_ledger::LogicalAppendCapability {
            self.inner.logical_append_capability()
        }

        fn supports_atomic_task_metadata_admission(&self) -> bool {
            self.inner.supports_atomic_task_metadata_admission()
        }

        fn acquire_workflow_mutation_lease(
            &self,
            run_id: &solaris_types::identity::RunId,
            owner_id: &str,
            now_unix_ms: i64,
        ) -> std::io::Result<$crate::runtime_ledger::WorkflowMutationLease> {
            self.inner
                .acquire_workflow_mutation_lease(run_id, owner_id, now_unix_ms)
        }

        fn renew_workflow_mutation_lease(
            &self,
            lease: &$crate::runtime_ledger::WorkflowMutationLease,
            now_unix_ms: i64,
        ) -> std::io::Result<$crate::runtime_ledger::WorkflowMutationLease> {
            self.inner.renew_workflow_mutation_lease(lease, now_unix_ms)
        }

        fn commit_workflow_restore(
            &self,
            lease: &$crate::runtime_ledger::WorkflowMutationLease,
            expected_sequence: u64,
            now_unix_ms: i64,
        ) -> std::io::Result<$crate::runtime_ledger::WorkflowRestoreCommit> {
            self.inner
                .commit_workflow_restore(lease, expected_sequence, now_unix_ms)
        }

        fn release_workflow_mutation_lease(
            &self,
            lease: &$crate::runtime_ledger::WorkflowMutationLease,
        ) -> std::io::Result<()> {
            self.inner.release_workflow_mutation_lease(lease)
        }

        fn compare_and_append(
            &self,
            run_id: &solaris_types::identity::RunId,
            durability: solaris_types::effect::DurabilityClass,
            record_type: &str,
            identity_fields: &[&str],
            payload: serde_json::Value,
        ) -> std::io::Result<$crate::runtime_ledger::LedgerRecord> {
            self.inner
                .compare_and_append(run_id, durability, record_type, identity_fields, payload)
        }

        fn append_under_workflow_lease(
            &self,
            lease: &$crate::runtime_ledger::WorkflowMutationLease,
            now_unix_ms: i64,
            durability: solaris_types::effect::DurabilityClass,
            record_type: &str,
            payload: serde_json::Value,
        ) -> std::io::Result<$crate::runtime_ledger::LedgerRecord> {
            self.inner
                .append_under_workflow_lease(lease, now_unix_ms, durability, record_type, payload)
        }

        fn admit_collaboration_tasks_for_root(
            &self,
            root_run_id: &solaris_types::identity::RunId,
            run_id: &solaris_types::identity::RunId,
            max_tasks: usize,
            tasks: &[solaris_types::runtime::TaskRecord],
        ) -> std::io::Result<Vec<$crate::runtime_ledger::LedgerRecord>> {
            self.inner
                .admit_collaboration_tasks_for_root(root_run_id, run_id, max_tasks, tasks)
        }

        fn admit_tasks_and_append(
            &self,
            root_run_id: &solaris_types::identity::RunId,
            run_id: &solaris_types::identity::RunId,
            max_tasks: usize,
            tasks: &[solaris_types::runtime::TaskRecord],
            records: &[(solaris_types::effect::DurabilityClass, String, serde_json::Value)],
        ) -> std::io::Result<Vec<$crate::runtime_ledger::LedgerRecord>> {
            self.inner
                .admit_tasks_and_append(root_run_id, run_id, max_tasks, tasks, records)
        }

        fn admit_tasks_and_append_under_workflow_lease(
            &self,
            lease: &$crate::runtime_ledger::WorkflowMutationLease,
            now_unix_ms: i64,
            root_run_id: &solaris_types::identity::RunId,
            max_tasks: usize,
            tasks: &[solaris_types::runtime::TaskRecord],
            records: &[(solaris_types::effect::DurabilityClass, String, serde_json::Value)],
        ) -> std::io::Result<Vec<$crate::runtime_ledger::LedgerRecord>> {
            self.inner.admit_tasks_and_append_under_workflow_lease(
                lease,
                now_unix_ms,
                root_run_id,
                max_tasks,
                tasks,
                records,
            )
        }

        fn compare_and_append_under_workflow_lease(
            &self,
            lease: &$crate::runtime_ledger::WorkflowMutationLease,
            now_unix_ms: i64,
            durability: solaris_types::effect::DurabilityClass,
            record_type: &str,
            identity_fields: &[&str],
            payload: serde_json::Value,
        ) -> std::io::Result<$crate::runtime_ledger::LedgerRecord> {
            self.inner.compare_and_append_under_workflow_lease(
                lease,
                now_unix_ms,
                durability,
                record_type,
                identity_fields,
                payload,
            )
        }
    };
}

pub(crate) use forward_compare_and_append;

macro_rules! unsupported_compare_and_append {
    () => {
        fn logical_append_capability(&self) -> $crate::runtime_ledger::LogicalAppendCapability {
            $crate::runtime_ledger::LogicalAppendCapability::Unsupported
        }

        fn acquire_workflow_mutation_lease(
            &self,
            _run_id: &solaris_types::identity::RunId,
            _owner_id: &str,
            _now_unix_ms: i64,
        ) -> std::io::Result<$crate::runtime_ledger::WorkflowMutationLease> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "test runtime ledger does not support Workflow mutation leases",
            ))
        }

        fn renew_workflow_mutation_lease(
            &self,
            _lease: &$crate::runtime_ledger::WorkflowMutationLease,
            _now_unix_ms: i64,
        ) -> std::io::Result<$crate::runtime_ledger::WorkflowMutationLease> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "test runtime ledger does not support Workflow mutation leases",
            ))
        }

        fn commit_workflow_restore(
            &self,
            _lease: &$crate::runtime_ledger::WorkflowMutationLease,
            _expected_sequence: u64,
            _now_unix_ms: i64,
        ) -> std::io::Result<$crate::runtime_ledger::WorkflowRestoreCommit> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "test runtime ledger does not support Workflow mutation leases",
            ))
        }

        fn release_workflow_mutation_lease(
            &self,
            _lease: &$crate::runtime_ledger::WorkflowMutationLease,
        ) -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "test runtime ledger does not support Workflow mutation leases",
            ))
        }

        fn compare_and_append(
            &self,
            _run_id: &solaris_types::identity::RunId,
            _durability: solaris_types::effect::DurabilityClass,
            _record_type: &str,
            _identity_fields: &[&str],
            _payload: serde_json::Value,
        ) -> std::io::Result<$crate::runtime_ledger::LedgerRecord> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "test runtime ledger does not support atomic logical record append",
            ))
        }

        fn append_under_workflow_lease(
            &self,
            _lease: &$crate::runtime_ledger::WorkflowMutationLease,
            _now_unix_ms: i64,
            _durability: solaris_types::effect::DurabilityClass,
            _record_type: &str,
            _payload: serde_json::Value,
        ) -> std::io::Result<$crate::runtime_ledger::LedgerRecord> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "test runtime ledger does not support fenced Workflow append",
            ))
        }

        fn compare_and_append_under_workflow_lease(
            &self,
            _lease: &$crate::runtime_ledger::WorkflowMutationLease,
            _now_unix_ms: i64,
            _durability: solaris_types::effect::DurabilityClass,
            _record_type: &str,
            _identity_fields: &[&str],
            _payload: serde_json::Value,
        ) -> std::io::Result<$crate::runtime_ledger::LedgerRecord> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "test runtime ledger does not support fenced logical Workflow append",
            ))
        }
    };
}

pub(crate) use unsupported_compare_and_append;

macro_rules! forward_workflow_mutation_lease {
    () => {
        fn acquire_workflow_mutation_lease(
            &self,
            run_id: &solaris_types::identity::RunId,
            owner_id: &str,
            now_unix_ms: i64,
        ) -> std::io::Result<$crate::runtime_ledger::WorkflowMutationLease> {
            self.inner
                .acquire_workflow_mutation_lease(run_id, owner_id, now_unix_ms)
        }

        fn renew_workflow_mutation_lease(
            &self,
            lease: &$crate::runtime_ledger::WorkflowMutationLease,
            now_unix_ms: i64,
        ) -> std::io::Result<$crate::runtime_ledger::WorkflowMutationLease> {
            self.inner.renew_workflow_mutation_lease(lease, now_unix_ms)
        }

        fn commit_workflow_restore(
            &self,
            lease: &$crate::runtime_ledger::WorkflowMutationLease,
            expected_sequence: u64,
            now_unix_ms: i64,
        ) -> std::io::Result<$crate::runtime_ledger::WorkflowRestoreCommit> {
            self.inner
                .commit_workflow_restore(lease, expected_sequence, now_unix_ms)
        }

        fn release_workflow_mutation_lease(
            &self,
            lease: &$crate::runtime_ledger::WorkflowMutationLease,
        ) -> std::io::Result<()> {
            self.inner.release_workflow_mutation_lease(lease)
        }

        fn append_under_workflow_lease(
            &self,
            lease: &$crate::runtime_ledger::WorkflowMutationLease,
            now_unix_ms: i64,
            durability: solaris_types::effect::DurabilityClass,
            record_type: &str,
            payload: serde_json::Value,
        ) -> std::io::Result<$crate::runtime_ledger::LedgerRecord> {
            self.inner
                .append_under_workflow_lease(lease, now_unix_ms, durability, record_type, payload)
        }

        fn admit_tasks_and_append_under_workflow_lease(
            &self,
            lease: &$crate::runtime_ledger::WorkflowMutationLease,
            now_unix_ms: i64,
            root_run_id: &solaris_types::identity::RunId,
            max_tasks: usize,
            tasks: &[solaris_types::runtime::TaskRecord],
            records: &[(solaris_types::effect::DurabilityClass, String, serde_json::Value)],
        ) -> std::io::Result<Vec<$crate::runtime_ledger::LedgerRecord>> {
            self.inner.admit_tasks_and_append_under_workflow_lease(
                lease,
                now_unix_ms,
                root_run_id,
                max_tasks,
                tasks,
                records,
            )
        }

        fn compare_and_append_under_workflow_lease(
            &self,
            lease: &$crate::runtime_ledger::WorkflowMutationLease,
            now_unix_ms: i64,
            durability: solaris_types::effect::DurabilityClass,
            record_type: &str,
            identity_fields: &[&str],
            payload: serde_json::Value,
        ) -> std::io::Result<$crate::runtime_ledger::LedgerRecord> {
            self.inner.compare_and_append_under_workflow_lease(
                lease,
                now_unix_ms,
                durability,
                record_type,
                identity_fields,
                payload,
            )
        }
    };
}

pub(crate) use forward_workflow_mutation_lease;
