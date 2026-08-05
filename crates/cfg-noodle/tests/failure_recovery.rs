//! Tests for storage failures that happen PARTWAY through an operation.
//!
//! A failure at the very start of a pass leaves nothing behind, but a failure in
//! the middle of one leaves the list holding results that only make sense as part
//! of a complete pass. These tests make sure such leftovers are not mistaken for
//! a finished scan, and that the list can be brought back to a working state.

use std::num::NonZeroU32;

use cfg_noodle::{
    StorageList, StorageListNode,
    error::{Error, LoadStoreError},
    test_utils::{TestStorage, TestStorageError},
};
use minicbor::{CborLen, Decode, Encode};
use mutex::raw_impls::cs::CriticalSectionRawMutex;
use test_log::test;
use tokio::task::{LocalSet, yield_now};

#[derive(Debug, Default, Encode, Decode, Clone, CborLen, PartialEq)]
struct SimpleConfig {
    #[n(0)]
    data: u64,
}

/// A storage holding two valid write records, five elements in total:
/// `Start(1), Data(111), End(1), Start(2), Data(222), End(2)`
fn two_record_flash() -> TestStorage {
    let mut flash = TestStorage::default();

    let mut wr = flash.start_write_record(NonZeroU32::new(1).unwrap());
    wr.add_data_elem("test/config1", &SimpleConfig { data: 111 });
    wr.end_write_record();

    let mut wr = flash.start_write_record(NonZeroU32::new(2).unwrap());
    wr.add_data_elem("test/config1", &SimpleConfig { data: 222 });
    wr.end_write_record();

    flash
}

/// A scan that fails partway has NOT seen the whole storage, so the records it did
/// find are not necessarily the newest ones. If those partial results are kept, the
/// next read trusts them and hydrates nodes from stale data.
#[test(tokio::test)]
async fn partial_scan_is_discarded() {
    let mut flash = two_record_flash();

    // Let the scan see all three elements of the OLDER write record, and fail
    // before it can reach the newer one.
    flash.reads_until_error = Some(3);

    static LIST: StorageList<CriticalSectionRawMutex, 3> = StorageList::new();
    static NODE: StorageListNode<SimpleConfig> = StorageListNode::new("test/config1");

    let mut buf = [0u8; 4096];

    let local = LocalSet::new();
    local
        .run_until(async move {
            let node = tokio::task::spawn_local(async { NODE.attach(&LIST).await.unwrap().load() });
            yield_now().await;
            assert!(!node.is_finished());

            let res = LIST.process_reads(&mut flash, &mut buf).await;
            assert!(matches!(
                res,
                Err(LoadStoreError::FlashRead(TestStorageError::FakeBadRead))
            ));

            // The storage is readable again
            flash.reads_until_error = None;
            LIST.process_reads(&mut flash, &mut buf).await.unwrap();

            yield_now().await;
            assert!(node.is_finished());

            // The newest record must win. Loading 111 would mean the aborted scan
            // was treated as complete.
            assert_eq!(node.await.unwrap(), SimpleConfig { data: 222 });
        })
        .await;
}

/// A collection that fails partway may have popped elements already, which
/// invalidates the cached element positions. The list has to notice that
/// collecting is still outstanding, and that it needs a fresh scan first.
#[test(tokio::test)]
async fn garbage_collect_failure_is_recoverable() {
    let mut flash = two_record_flash();

    static LIST: StorageList<CriticalSectionRawMutex, 3> = StorageList::new();
    static NODE: StorageListNode<SimpleConfig> = StorageListNode::new("test/config1");

    let mut buf = [0u8; 4096];

    let local = LocalSet::new();
    local
        .run_until(async move {
            let node = tokio::task::spawn_local(async { NODE.attach(&LIST).await.unwrap() });
            yield_now().await;
            LIST.process_reads(&mut flash, &mut buf).await.unwrap();
            yield_now().await;
            let _node = node.await.unwrap();

            // Fail partway through the collection pass
            flash.reads_until_error = Some(2);
            let res = LIST.process_garbage(&mut flash, &mut buf).await;
            assert!(matches!(
                res,
                Err(LoadStoreError::FlashRead(TestStorageError::FakeBadRead))
            ));

            // The storage is usable again
            flash.reads_until_error = None;

            // Collecting is still outstanding, and reporting "nothing to do" here
            // would hide the earlier failure. Instead we are told what is missing.
            let err = LIST
                .process_garbage(&mut flash, &mut buf)
                .await
                .unwrap_err();
            assert_eq!(err, LoadStoreError::AppError(Error::NeedsFirstRead));

            // Writes are blocked for the same reason...
            let err = LIST.process_writes(&mut flash, &mut buf).await.unwrap_err();
            assert_eq!(err, LoadStoreError::AppError(Error::NeedsFirstRead));

            // ...and rebuilding the cache puts everything back into working order
            LIST.process_reads(&mut flash, &mut buf).await.unwrap();
            LIST.process_garbage(&mut flash, &mut buf).await.unwrap();
            LIST.process_writes(&mut flash, &mut buf).await.unwrap();
        })
        .await;
}
