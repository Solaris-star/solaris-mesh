use std::fs;

use tempfile::tempdir;

use super::MemoryService;

#[test]
fn legacy_markdown_is_imported_once_and_the_source_stays_unchanged() {
    let temp = tempdir().unwrap();
    let memory_dir = temp.path().join("memory");
    fs::create_dir_all(&memory_dir).unwrap();
    let source = memory_dir.join("project_release.md");
    let original = "---\nname: release\ntype: project\ndescription: release rules\n---\n\nrun all tests";
    fs::write(&source, original).unwrap();
    let service = MemoryService::open(memory_dir.join("memory.sqlite3")).unwrap();

    assert_eq!(service.import_legacy_directory(&memory_dir).unwrap(), 1);
    assert_eq!(service.import_legacy_directory(&memory_dir).unwrap(), 0);
    assert_eq!(service.list().unwrap().len(), 1);
    assert_eq!(service.list().unwrap()[0].content, "run all tests");
    assert_eq!(fs::read_to_string(&source).unwrap(), original);
}

#[test]
fn changed_legacy_source_is_not_reimported_after_completion() {
    let temp = tempdir().unwrap();
    let memory_dir = temp.path().join("memory");
    fs::create_dir_all(&memory_dir).unwrap();
    let source = memory_dir.join("reference.md");
    fs::write(&source, "original reference").unwrap();
    let service = MemoryService::open(memory_dir.join("memory.sqlite3")).unwrap();
    service.import_legacy_directory(&memory_dir).unwrap();

    fs::write(&source, "changed after migration").unwrap();

    assert_eq!(service.import_legacy_directory(&memory_dir).unwrap(), 0);
    assert_eq!(service.list().unwrap()[0].content, "original reference");
}

#[test]
fn oversized_sources_are_not_imported() {
    let temp = tempdir().unwrap();
    let memory_dir = temp.path().join("memory");
    fs::create_dir_all(&memory_dir).unwrap();
    fs::write(memory_dir.join("large.md"), vec![b'x'; 1024 * 1024 + 64 * 1024 + 1]).unwrap();
    let service = MemoryService::open(memory_dir.join("memory.sqlite3")).unwrap();

    assert_eq!(service.import_legacy_directory(&memory_dir).unwrap(), 0);
    assert!(service.list().unwrap().is_empty());
}
