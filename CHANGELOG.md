# Changelog

(DD-MM-YY)

## Unreleased

- Fixed a scan that fails partway being cached as if it were complete. The records found
  before the failure are not necessarily the newest ones, so a following read could
  hydrate nodes from an older write record and then overwrite the newer one.
- Fixed `process_garbage` reporting "nothing to do" after a failed collection. It now
  keeps the collection outstanding and returns `NeedsFirstRead` until a read has rebuilt
  the cache, instead of silently leaving writes blocked forever.
- Fixed default values written back by `attach` never being signalled to the worker task.
  With the default worker, they were only persisted on shutdown. Note that this means a
  node that is missing from flash now causes a write record on first boot, as documented.
- Fixed the cached position of a verified write record covering one element too many.
- `default_worker_task` now retries reads (and the writes that depend on them) after a
  failure, instead of waiting for a signal that will never come. This prevents `attach()`
  from waiting forever after a transient storage error.

## 0.8.0 21-07-26

- *Breaking:* Updated to sequential-storage 8

## 0.7.0 16-04-26

- *Breaking:* Updated embassy dependencies
- *Breaking:* Default worker tasks is now behind a feature flag
- Added new feature flag that forwards the embassy-time generic-queue flag

## 0.6.0 16-12-25

- *Breaking:* Added const generic to list that controls how much redundancy is stored. To keep the same behavior as before, use the value 3.
- *Breaking:* Updated to sequential-storage 7

## 0.5.0 03-09-25

- *Breaking:* Update to embassy-time 0.5

## 0.4.0 31-07-25

- *Breaking:* Updated sequential-storage to 5.0.0 and minicbor to 2.0.0
  - Both are very minor major releases
- Removed 'static trait bounds for NdlDataStorage for Flash and NdlElemIter
- Make it compile on windows
- Expose the extracted kv-pair to the outside world so people can look at the cbor bytes
