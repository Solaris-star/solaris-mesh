use super::*;
use std::cell::RefCell;

#[test]
fn psec_exports_own_their_exact_nul_terminated_symbols() {
    assert_eq!(PsecExport::Create.symbol(), c"CreateProcessSecurityEnvironment");
    assert_eq!(
        PsecExport::QuerySupport.symbol(),
        c"QueryProcessSecurityEnvironmentSupport"
    );
    assert_eq!(PsecExport::Close.symbol(), c"CloseProcessSecurityEnvironment");
}

struct FakePsecProbe {
    load_error: Option<PsecProbeStatus>,
    missing_export: Option<PsecExport>,
    resolved: RefCell<Vec<(PsecExport, &'static CStr)>>,
    query_hresult: i32,
    query_flags: u64,
}

struct FakeCreateExport;
struct FakeQuerySupportExport;
struct FakeCloseExport;

impl FakePsecProbe {
    fn supported(query_flags: u64) -> Self {
        Self {
            load_error: None,
            missing_export: None,
            resolved: RefCell::new(Vec::new()),
            query_hresult: 0,
            query_flags,
        }
    }

    fn with_load_error(mut self, status: PsecProbeStatus) -> Self {
        self.load_error = Some(status);
        self
    }

    fn with_missing_export(mut self, export: PsecExport) -> Self {
        self.missing_export = Some(export);
        self
    }

    fn resolve(&self, export: PsecExport) -> Result<(), PsecProbeStatus> {
        self.resolved.borrow_mut().push((export, export.symbol()));
        if self.missing_export == Some(export) {
            return Err(PsecProbeStatus::ExportMissing { export, code: 127 });
        }
        Ok(())
    }
}

impl PsecProbeAdapter for FakePsecProbe {
    type Module = ();
    type CreateExport = FakeCreateExport;
    type QuerySupportExport = FakeQuerySupportExport;
    type CloseExport = FakeCloseExport;

    fn load_processmodel_from_system32(&self) -> Result<Self::Module, PsecProbeStatus> {
        self.load_error.map_or(Ok(()), Err)
    }

    fn resolve_create(&self, _module: &Self::Module) -> Result<Self::CreateExport, PsecProbeStatus> {
        self.resolve(PsecExport::Create)?;
        Ok(FakeCreateExport)
    }

    fn resolve_query_support(&self, _module: &Self::Module) -> Result<Self::QuerySupportExport, PsecProbeStatus> {
        self.resolve(PsecExport::QuerySupport)?;
        Ok(FakeQuerySupportExport)
    }

    fn resolve_close(&self, _module: &Self::Module) -> Result<Self::CloseExport, PsecProbeStatus> {
        self.resolve(PsecExport::Close)?;
        Ok(FakeCloseExport)
    }

    fn query_support(&self, _query: Self::QuerySupportExport) -> (i32, u64) {
        (self.query_hresult, self.query_flags)
    }
}

#[test]
fn fake_loader_and_resolver_failures_travel_through_the_production_probe() {
    let load_status = PsecProbeStatus::DllUnavailable {
        operation: DllProbeOperation::LoadProcessModel,
        code: 126,
    };
    let load_failure = FakePsecProbe::supported(0).with_load_error(load_status);
    assert_eq!(probe_network_status_with(&load_failure), load_status);
    assert!(load_failure.resolved.borrow().is_empty());

    let missing_close = FakePsecProbe::supported(0).with_missing_export(PsecExport::Close);
    assert_eq!(
        probe_network_status_with(&missing_close),
        PsecProbeStatus::ExportMissing {
            export: PsecExport::Close,
            code: 127,
        }
    );
    assert_eq!(
        *missing_close.resolved.borrow(),
        [
            (PsecExport::Create, c"CreateProcessSecurityEnvironment"),
            (PsecExport::QuerySupport, c"QueryProcessSecurityEnvironmentSupport"),
            (PsecExport::Close, c"CloseProcessSecurityEnvironment"),
        ]
    );
}

#[test]
fn exports_and_query_do_not_replace_the_functional_network_probe() {
    let probe = FakePsecProbe::supported(u64::MAX);
    let capability = PsecCapability {
        status: probe_network_status_with(&probe),
    };

    assert_eq!(
        *probe.resolved.borrow(),
        [
            (PsecExport::Create, c"CreateProcessSecurityEnvironment"),
            (PsecExport::QuerySupport, c"QueryProcessSecurityEnvironmentSupport"),
            (PsecExport::Close, c"CloseProcessSecurityEnvironment"),
        ]
    );
    assert!(!capability.is_fully_proven(), "probe: {capability:?}");
    assert!(matches!(
        capability.status,
        PsecProbeStatus::FunctionalProbeIncomplete { .. }
    ));
}

#[test]
fn query_errors_keep_distinct_safe_categories() {
    assert_eq!(
        classify_query(E_NOTIMPL, 0),
        QueryOutcome::Unsupported {
            kind: QueryUnsupportedKind::NotImplemented,
            hresult: E_NOTIMPL,
        }
    );
    assert_eq!(
        classify_query(HRESULT_FROM_WIN32_CALL_NOT_IMPLEMENTED, 0),
        QueryOutcome::Unsupported {
            kind: QueryUnsupportedKind::Win32CallNotImplemented,
            hresult: HRESULT_FROM_WIN32_CALL_NOT_IMPLEMENTED,
        }
    );
    assert_eq!(
        classify_query(HRESULT_FROM_WIN32_NOT_SUPPORTED, 0),
        QueryOutcome::Unsupported {
            kind: QueryUnsupportedKind::NotSupported,
            hresult: HRESULT_FROM_WIN32_NOT_SUPPORTED,
        }
    );
    assert!(matches!(classify_query(i32::MIN, 0), QueryOutcome::Failed { .. }));
}

#[test]
fn functional_failure_identifies_the_unproven_step_without_becoming_full() {
    let evidence = FunctionalProbeEvidence {
        create: FunctionalProbeState::Passed,
        close: FunctionalProbeState::Failed { code: 5 },
        process_attribute: FunctionalProbeState::Passed,
        packaged_proxy_peer: FunctionalProbeState::Passed,
        wfp_filtering: FunctionalProbeState::Passed,
    };
    let capability = PsecCapability {
        status: classify_functional(QueryOutcome::Supported { flags: 1 }, evidence),
    };

    assert!(!capability.is_fully_proven());
    assert_eq!(
        capability.status,
        PsecProbeStatus::FunctionalProbeFailed {
            support_flags: 1,
            evidence,
        }
    );
}

#[test]
fn current_host_probe_cannot_claim_the_unimplemented_network_path() {
    let capability = probe_network_capability();

    eprintln!("PSEC host probe: {capability:?}");
    assert!(!capability.is_fully_proven(), "host probe: {capability:?}");
}
