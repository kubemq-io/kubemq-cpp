/// @file export.h
/// @brief DLL export/import macros for the KubeMQ C++ SDK.
///
/// Defines the KUBEMQ_API macro used on all public classes and functions:
/// - When KUBEMQ_STATIC is defined: KUBEMQ_API is empty (static linking).
/// - When KUBEMQ_EXPORTS is defined: KUBEMQ_API is dllexport (MSVC) or
///   visibility("default") (GCC/Clang) -- used when building the shared library.
/// - Otherwise: KUBEMQ_API is dllimport (MSVC) or empty (GCC/Clang) -- used
///   by consumers of the shared library.
#pragma once

#if defined(KUBEMQ_STATIC)
#define KUBEMQ_API
#elif defined(KUBEMQ_EXPORTS)
#if defined(_MSC_VER)
#define KUBEMQ_API __declspec(dllexport)
#else
#define KUBEMQ_API __attribute__((visibility("default")))
#endif
#else
#if defined(_MSC_VER)
#define KUBEMQ_API __declspec(dllimport)
#else
#define KUBEMQ_API
#endif
#endif
