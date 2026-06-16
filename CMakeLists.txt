#
# Copyright (c) 2022 ZettaScale Technology
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
# which is available at https://www.apache.org/licenses/LICENSE-2.0.
#
# SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
#
# Contributors:
# ZettaScale Zenoh Team, <zenoh@zettascale.tech>
#

# ROS2 package build file (waiting for an "ament_rust" built type)
cmake_minimum_required(VERSION 3.8)
project(zenoh_bridge_ros2dds)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# find dependencies
find_package(ament_cmake REQUIRED)

# uncomment the following section in order to fill in
# further dependencies manually.
# find_package(<dependency> REQUIRED)
set(RUST_TARGET_DIR ${CMAKE_BINARY_DIR}/target)

FILE(GLOB RUST_SRC_FILES
  ${CMAKE_SOURCE_DIR}/zenoh-bridge-ros2dds/src/*.rs
  ${CMAKE_SOURCE_DIR}/zenoh-plugin-ros2dds/src/*.rs
  ${CMAKE_SOURCE_DIR}/Cargo.lock)

# Locate cargo. PREFER a rustup install (~/.cargo/bin) over a distro /usr/bin/cargo
find_program(CARGO_EXECUTABLE cargo PATHS "$ENV{HOME}/.cargo/bin" NO_DEFAULT_PATH)
if(NOT CARGO_EXECUTABLE)
  find_program(CARGO_EXECUTABLE cargo)  # fall back to PATH (e.g. /usr/bin/cargo)
endif()
if(NOT CARGO_EXECUTABLE)
  message(FATAL_ERROR
    "cargo not found. This package builds a Rust crate and needs rustup with the "
    "toolchain pinned in rust-toolchain.toml (currently 1.93.0). Install rustup:\n"
    "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y\n"
    "  source \"$ENV{HOME}/.cargo/env\"\n"
    "then ensure ~/.cargo/bin is on PATH for the build.")
endif()

# Verify the toolchain is new enough for the v4 Cargo.lock (needs cargo >= 1.78),
execute_process(
  COMMAND ${CMAKE_COMMAND} -E chdir "${CMAKE_SOURCE_DIR}" ${CARGO_EXECUTABLE} --version
  OUTPUT_VARIABLE _cargo_version_out
  ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+" _cargo_version "${_cargo_version_out}")
if(_cargo_version AND _cargo_version VERSION_LESS "1.78")
  message(FATAL_ERROR
    "cargo ${_cargo_version} at '${CARGO_EXECUTABLE}' is too old: this package's "
    "Cargo.lock is v4, which needs cargo >= 1.78. The distro cargo is likely being "
    "used instead of rustup. Install rustup and ensure ~/.cargo/bin precedes /usr/bin "
    "on PATH (or remove the apt 'cargo'/'rustc' packages):\n"
    "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y")
endif()
message(STATUS "Using cargo ${_cargo_version} at ${CARGO_EXECUTABLE}")

add_custom_command(
  OUTPUT
  ${RUST_TARGET_DIR}/release/zenoh-bridge-ros2dds
  # Run cargo with the working directory set to the source root so rustup finds
  # the repo's rust-toolchain.toml
  COMMAND ${CMAKE_COMMAND} -E chdir "${CMAKE_SOURCE_DIR}"
          ${CARGO_EXECUTABLE} build --release --manifest-path "${CMAKE_SOURCE_DIR}/zenoh-bridge-ros2dds/Cargo.toml" --target-dir "${RUST_TARGET_DIR}"
  DEPENDS
  ${RUST_SRC_FILES}
)

add_custom_target(
  build_crate ALL
  DEPENDS
  ${RUST_TARGET_DIR}/release/zenoh-bridge-ros2dds
)

macro(INSTALL_ZENOH src_path dst_path)
  install(FILES
    ${src_path}
    RENAME zenoh_bridge_ros2dds
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
    DESTINATION ${dst_path}
  )
endmacro(INSTALL_ZENOH)

INSTALL_ZENOH(${RUST_TARGET_DIR}/release/zenoh-bridge-ros2dds lib/${PROJECT_NAME})

INSTALL_ZENOH(${RUST_TARGET_DIR}/release/zenoh-bridge-ros2dds bin)

# Also install the executable under its original hyphenated name so launchers
# that call `zenoh-bridge-ros2dds` resolve it directly,
# in addition to the ROS-convention `zenoh_bridge_ros2dds` installed above.
install(PROGRAMS
  ${RUST_TARGET_DIR}/release/zenoh-bridge-ros2dds
  DESTINATION bin
)

ament_package()
