use std::os::windows::io::RawHandle;

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectBasicAccountingInformation,
    JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};

pub(crate) struct JobObject {
    job: HANDLE,
}

unsafe impl Send for JobObject {}
unsafe impl Sync for JobObject {}

impl JobObject {
    pub(crate) fn assign_and_resume(child_raw: RawHandle, pid: u32) -> std::result::Result<Self, String> {
        let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if job.is_null() {
            return Err(format!("CreateJobObjectW failed: {}", std::io::Error::last_os_error()));
        }

        let this = Self { job };
        this.set_kill_on_close(true)?;

        let ok = unsafe { AssignProcessToJobObject(job, child_raw as HANDLE) };
        if ok == 0 {
            return Err(format!(
                "AssignProcessToJobObject failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        let mut assigned = 0;
        if unsafe { IsProcessInJob(child_raw as HANDLE, job, &mut assigned) } == 0 {
            let _ = this.terminate();
            return Err(format!("IsProcessInJob failed: {}", std::io::Error::last_os_error()));
        }
        if assigned == 0 {
            let _ = this.terminate();
            return Err("child process was not assigned to the requested Job Object".to_owned());
        }

        if let Err(error) = resume_threads(pid) {
            let _ = this.terminate();
            return Err(format!("resume failed: {error}"));
        }

        Ok(this)
    }

    pub(crate) fn terminate(&self) -> std::io::Result<()> {
        let ok = unsafe { TerminateJobObject(self.job, 1) };
        if ok == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(crate) fn is_empty(&self) -> std::io::Result<bool> {
        let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        let ok = unsafe {
            QueryInformationJobObject(
                self.job,
                JobObjectBasicAccountingInformation,
                (&raw mut accounting).cast(),
                u32::try_from(std::mem::size_of_val(&accounting))
                    .map_err(|_| std::io::Error::other("Windows Job accounting structure is too large"))?,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(accounting.ActiveProcesses == 0)
        }
    }

    fn set_kill_on_close(&self, enabled: bool) -> std::result::Result<(), String> {
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags = if enabled { JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE } else { 0 };
        let ok = unsafe {
            SetInformationJobObject(
                self.job,
                JobObjectExtendedLimitInformation,
                &limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION as *const std::ffi::c_void,
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if ok == 0 {
            Err(format!(
                "SetInformationJobObject failed: {}",
                std::io::Error::last_os_error()
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for JobObject {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.job) };
    }
}

fn resume_threads(pid: u32) -> std::result::Result<(), String> {
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(format!(
            "CreateToolhelp32Snapshot failed: {}",
            std::io::Error::last_os_error()
        ));
    }

    let result = resume_threads_from_snapshot(snapshot, pid);
    unsafe { CloseHandle(snapshot) };
    result
}

fn resume_threads_from_snapshot(snapshot: HANDLE, pid: u32) -> std::result::Result<(), String> {
    let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
    entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;

    let mut ok = unsafe { Thread32First(snapshot, &mut entry) };
    if ok == 0 {
        return Err(format!("Thread32First failed: {}", std::io::Error::last_os_error()));
    }

    let mut resumed = 0_u32;
    while ok != 0 {
        if entry.th32OwnerProcessID == pid {
            resume_thread(entry.th32ThreadID)?;
            resumed += 1;
        }
        entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;
        ok = unsafe { Thread32Next(snapshot, &mut entry) };
    }

    if resumed == 0 {
        return Err(format!("no threads found for child process {pid}"));
    }

    Ok(())
}

fn resume_thread(thread_id: u32) -> std::result::Result<(), String> {
    let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, thread_id) };
    if thread.is_null() {
        return Err(format!(
            "OpenThread({thread_id}) failed: {}",
            std::io::Error::last_os_error()
        ));
    }

    let resume_result = unsafe { ResumeThread(thread) };
    unsafe { CloseHandle(thread) };

    if resume_result == u32::MAX {
        return Err(format!(
            "ResumeThread({thread_id}) failed: {}",
            std::io::Error::last_os_error()
        ));
    }

    Ok(())
}
