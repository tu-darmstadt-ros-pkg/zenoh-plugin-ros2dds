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

# Locate cargo. Fail early with actionable instructions 
find_program(CARGO_EXECUTABLE cargo HINTS "$ENV{HOME}/.cargo/bin")
if(NOT CARGO_EXECUTABLE)
  message(FATAL_ERROR
    "cargo not found. This package builds a Rust crate and needs rustup with the "
    "toolchain pinned in rust-toolchain.toml (currently 1.93.0). Install rustup:\n"
    "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y\n"
    "  source \"$ENV{HOME}/.cargo/env\"\n"
    "then ensure ~/.cargo/bin is on PATH for the build.")
endif()

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
