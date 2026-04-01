# Contributing to KubeMQ C++ SDK

Thank you for considering contributing to the KubeMQ C++ SDK. This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- C++17 compiler (GCC 9+, Clang 9+, or MSVC 2019+)
- CMake 3.16+
- vcpkg (recommended) or Conan 2.x
- A running KubeMQ server for integration tests

### Building from Source

```bash
# Clone
git clone https://github.com/kubemq-io/kubemq-cpp.git
cd kubemq-cpp

# Install dependencies (vcpkg)
vcpkg install grpc protobuf gtest cpp-httplib nlohmann-json

# Configure (Debug for development)
cmake -B build -S . \
    -DCMAKE_TOOLCHAIN_FILE=[vcpkg-root]/scripts/buildsystems/vcpkg.cmake \
    -DCMAKE_BUILD_TYPE=Debug \
    -DKUBEMQ_BUILD_TESTS=ON \
    -DKUBEMQ_BUILD_EXAMPLES=ON \
    -DKUBEMQ_BUILD_BURNIN=ON

# Build
cmake --build build --parallel
```

### Running Tests

```bash
# Unit tests (no broker required)
cd build && ctest --label-exclude integration --output-on-failure

# Integration tests (requires broker on localhost:50000)
cd build && ctest --label-regex integration --output-on-failure

# Coverage report
cmake -B build-cov -S . \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_FLAGS="--coverage"
cmake --build build-cov
cd build-cov && ctest --label-exclude integration
lcov --capture --directory . --output-file coverage.info
lcov --remove coverage.info '/usr/*' '*/generated/*' --output-file coverage.info
genhtml coverage.info --output-directory coverage-report
```

## Code Style

This project follows the **Google C++ Style Guide** with minor modifications. The style is enforced via `.clang-format` and `.clang-tidy`.

### Key Conventions

- **Naming**: `ClassName`, `MethodName()`, `member_variable_`, `kConstantName`, `local_variable`
- **Indentation**: 4 spaces (no tabs)
- **Column limit**: 100 characters
- **Headers**: Use `#pragma once` for include guards
- **Namespaces**: `namespace kubemq { inline namespace v1 { ... } }` for public API
- **Error handling**: Use `Status` and `StatusOr<T>` return types; no exceptions in public API
- **PIMPL**: Use on `Client` and all handle/subscription classes for ABI stability
- **Include order**: Standard library, third-party, project headers (sorted within each group)

### Formatting

```bash
# Format all source files
find include src tests examples burnin -name '*.h' -o -name '*.cc' | xargs clang-format -i

# Run static analysis
clang-tidy -p build src/*.cc
```

## Architecture

The codebase follows a 3-layer architecture:

1. **Public API** (`include/kubemq/`) -- User-facing types, `KUBEMQ_API` exported
2. **Internal Layer** (`src/internal/`) -- Transport, middleware, type conversions
3. **Proto Layer** (`src/proto/`) -- gRPC/protobuf codegen

### Key Design Decisions

- Single `Client` class (matches Go SDK pattern)
- gRPC Callback API for async operations
- `StatusOr<T>` using `std::variant` for error handling
- Ring buffer (1024 capacity) for message buffering during reconnection
- Per-subscription delivery buffer (size 10, matches Go SDK)

## Testing Guidelines

- **80%+ line coverage target** for unit tests
- Every send operation in integration tests must verify receipt
- Unit tests use Google Test + Google Mock
- Integration tests require a live broker on `localhost:50000`
- Use descriptive test names: `TEST(ClassName, WhatItTests_ExpectedBehavior)`

## Pull Request Guidelines

1. Create a feature branch from `main`
2. Write tests for new functionality
3. Ensure all existing tests pass
4. Run `clang-format` on modified files
5. Write a clear PR description explaining what and why
6. Reference any related issues

### PR Checklist

- [ ] Code compiles without warnings on GCC, Clang, and MSVC
- [ ] Unit tests added/updated and passing
- [ ] Integration tests passing (if applicable)
- [ ] `clang-format` applied
- [ ] Documentation updated (if public API changed)
- [ ] No breaking API changes (or documented migration path)

## Release Process

1. Update version in `CMakeLists.txt`, `types.h`, and `vcpkg.json`
2. Update `CHANGELOG.md`
3. Create a tagged release
4. Build and test on all supported platforms

## Questions?

Open an issue on GitHub or reach out to the KubeMQ team at [kubemq.io](https://kubemq.io).
