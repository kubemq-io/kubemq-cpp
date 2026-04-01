# Migration Guide

## v1.0.0 (Initial Release)

This is the first release of the KubeMQ C++ SDK. No migration steps are needed.

### Coming from another KubeMQ SDK?

The C++ SDK follows the same patterns as other KubeMQ SDKs with C++-specific idioms:

- **Error handling:** Uses `Status`/`StatusOr<T>` instead of exceptions
- **Builder pattern:** All message types use `Builder` classes (e.g., `Event::Builder`)
- **Resource management:** RAII -- streams and subscriptions clean up on destruction
- **Memory:** Uses `std::unique_ptr` for client and handle ownership
- **Threading:** All public methods on `Client` are thread-safe
- **Namespacing:** All types are in `kubemq::v1` (accessible as `kubemq::` via inline namespace)

### Migration Notes for Future Releases

This file will be updated with breaking changes and migration steps for each release.
See [CHANGELOG.md](CHANGELOG.md) for detailed release notes.
