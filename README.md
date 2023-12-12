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
- [x] Spin lock. (No deadlock, lock for a dozen instructions, no lock release on error, timeout in minutes (very long blocking))
- [ ] Lightweight locks.
- [ ] Regular lock
- [ ]
### Lock-free
- [ ] CAS lock-free queue.

## Extra
- [ ] Cache line padding investigation.
