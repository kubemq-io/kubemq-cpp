from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout


class KubeMQCppConan(ConanFile):
    name = "kubemq-cpp"
    version = "1.0.0"
    license = "Apache-2.0"
    author = "KubeMQ"
    url = "https://github.com/kubemq-io/kubemq-cpp"
    homepage = "https://kubemq.io"
    description ="High-performance C++17 client SDK for KubeMQ message broker -- supports Events, Events Store, Commands, Queries, and Queues"
    topics = ("messaging", "grpc", "pubsub", "queue", "kubemq")
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "with_tests": [True, False],
        "with_examples": [True, False],
        "with_burnin": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "with_tests": False,
        "with_examples": False,
        "with_burnin": False,
    }
    exports_sources = (
        "CMakeLists.txt",
        "cmake/*",
        "include/*",
        "src/*",
    )

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def layout(self):
        cmake_layout(self)

    def requirements(self):
        self.requires("grpc/1.78.0")
        self.requires("protobuf/5.29.0")
        self.requires("nlohmann_json/3.11.3")
        if self.options.with_tests:
            self.requires("gtest/1.14.0")
        if self.options.with_burnin:
            self.requires("cpp-httplib/0.15.3")

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()
        tc = CMakeToolchain(self)
        tc.variables["KUBEMQ_BUILD_SHARED"] = self.options.shared
        tc.variables["KUBEMQ_BUILD_STATIC"] = not self.options.shared
        tc.variables["KUBEMQ_BUILD_TESTS"] = self.options.with_tests
        tc.variables["KUBEMQ_BUILD_EXAMPLES"] = self.options.with_examples
        tc.variables["KUBEMQ_BUILD_BURNIN"] = self.options.with_burnin
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["kubemq"]
        self.cpp_info.set_property("cmake_file_name", "KubeMQCpp")
        self.cpp_info.set_property("cmake_target_name", "KubeMQ::kubemq")
        self.cpp_info.requires = ["grpc::grpc++", "protobuf::libprotobuf-lite",
                                  "nlohmann_json::nlohmann_json"]
