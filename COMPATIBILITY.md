# Compatibility Matrix

## Compiler Support

| Compiler | Minimum Version | Tested Versions | Notes |
|----------|----------------|-----------------|-------|
| GCC | 9.0 | 9, 10, 11, 12, 13, 14 | Primary Linux compiler |
| Clang | 9.0 | 9, 10, 11, 12, 13, 14, 15, 16, 17, 18 | Primary macOS compiler |
| MSVC | 2019 (19.20) | 2019, 2022 | Windows support |
| Apple Clang | 12.0 | 12, 13, 14, 15 | macOS Xcode |

## Operating System Support

| OS | Architecture | Status | Notes |
|----|-------------|--------|-------|
| Ubuntu 20.04+ | x86_64 | Supported | Primary development platform |
| Ubuntu 20.04+ | aarch64 | Supported | ARM64 Linux |
| Debian 11+ | x86_64 | Supported | |
| CentOS Stream 9+ | x86_64 | Supported | |
| Fedora 38+ | x86_64 | Supported | |
| Alpine 3.18+ | x86_64 | Supported | musl libc |
| macOS 12+ | x86_64 | Supported | Intel Mac |
| macOS 12+ | arm64 | Supported | Apple Silicon |
| Windows 10+ | x86_64 | Supported | MSVC only |
| Windows 11 | arm64 | Experimental | Limited testing |

## C++ Standard

- **Required**: C++17 (`-std=c++17`)
- **Extensions**: Disabled (`CMAKE_CXX_EXTENSIONS=OFF`)

### C++17 Features Used

| Feature | Usage |
|---------|-------|
| `std::variant` | `StatusOr<T>` implementation |
| `std::optional` | Internal optional values |
| `std::string_view` | Internal string processing |
| `if constexpr` | Template metaprogramming |
| `std::mutex` / `std::shared_mutex` | Thread safety |
| `[[nodiscard]]` | Prevent ignoring return values |
| `inline namespace` | API versioning (`inline namespace v1`) |
| Structured bindings | Internal code readability |
| `std::chrono` literals | Duration constants |

## Dependency Versions

| Dependency | Minimum | Recommended | License |
|-----------|---------|-------------|---------|
| gRPC | 1.78.0 | Latest stable | Apache-2.0 |
| Protobuf | 5.29.0 | Latest stable | BSD-3-Clause |
| nlohmann/json | 3.11.0 | Latest stable | MIT |
| Google Test | 1.14.0 | Latest stable | BSD-3-Clause |
| cpp-httplib | 0.15.0 | Latest stable | MIT |
| CMake | 3.16 | 3.25+ | BSD-3-Clause |

## Build System Support

| Method | Status | Notes |
|--------|--------|-------|
| vcpkg | Supported | Recommended |
| Conan 2.x | Supported | `conanfile.py` included |
| CMake FetchContent | Supported | For embedding |
| Manual source build | Supported | |

## Known Limitations

1. **C++20 coroutines**: Not supported. The SDK uses gRPC Callback API for async operations.
2. **Windows ARM64**: Experimental. Depends on gRPC ARM64 Windows support.
3. **32-bit platforms**: Not tested or supported.
4. **Static linking on macOS**: May require additional linker flags for gRPC dependencies.
5. **Alpine Linux**: Requires `musl`-compatible gRPC build.
