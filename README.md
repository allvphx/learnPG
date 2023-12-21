# learnPG
Learning PostgreSQL by reimplementing some of its functions in Golang.

# List
## Common
- [x] Datum type.

## Access
### Tuple
- [x] Tuple creation and basic organization.
- [ ] Tuple descriptors.
- [ ] Attribute constraints.

## Storage
### Lock Manager
- [x] Spin lock.
  - [x] Basic implementation: no deadlock, lock for a dozen instructions, no lock release on error, timeout in minutes (very long blocking)
  - [x] Spin lock delay controls for high contention locks.
- [x] Basic PROC.
  - [x] PROC list
  - [x] Multable iterator
- [ ] Lightweight locks.
  - [x] spin lock style FetchExecute, compiler barrels, etc.
  - [x] lock tranches.
  - [x] wait list locks and lock flags.
- [ ] Regular lock
- [ ]
### Lock-free
- [ ] CAS lock-free queue.

## Extra
- [ ] Cache line padding investigation.
