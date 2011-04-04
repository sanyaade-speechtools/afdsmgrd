#
# CMakeLists.txt -- by Dario Berzano <dario.berzano@gmail.com>
#
# This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
#
# Main CMake build and install file for afdsmgrd. Other CMakeLists.txt may be
# included in subdirectories.
#

#
# Common parameters
#

# Name of the project and minimum version required
project (afdsmgrd)
cmake_minimum_required (VERSION 2.6)

# Where to find additional modules
set (CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules")

# The build type
#set (CMAKE_BUILD_TYPE "Debug")

# Tells the user the installation path
message (STATUS "Installation prefix: ${CMAKE_INSTALL_PREFIX}")

#
# Supported build types
#

if (${CMAKE_BUILD_TYPE} STREQUAL "Release")

  message (STATUS "Building in release mode")
  message (STATUS "C++ common build flags: ${CMAKE_CXX_FLAGS_RELEASE}")

elseif (${CMAKE_BUILD_TYPE} STREQUAL "Debug")

  message (STATUS "Building in debug mode")
  message (STATUS "C++ common build flags: ${CMAKE_CXX_FLAGS_DEBUG}")

else ()

  message (FATAL "Unsupported build type: ${CMAKE_BUILD_TYPE}")

endif ()

#
# A handy macro to copy a file in install phase without overwriting it. As
# destination directory, both absolute and relative paths are supported.
# Absolute paths are left intact, while relative paths get the CMAKE install
# prefix prepended
#

#macro (install_dont_overwrite SRCFILE DESTDIR)
#
#  get_filename_component (_FILENAME ${SRCFILE} NAME)
#  get_filename_component (_FULLSRC ${SRCFILE} ABSOLUTE)
#
#  if (IS_ABSOLUTE ${DESTDIR})
#    get_filename_component (_FULLDEST ${DESTDIR}/${_FILENAME} ABSOLUTE)
#  else ()
#    get_filename_component (_FULLDEST ${CMAKE_INSTALL_PREFIX}/${DESTDIR}/${_FILENAME} ABSOLUTE)
#  endif ()
#
#  install (CODE "
#    if (NOT EXISTS ${_FULLDEST})
#      message (STATUS \"Installing: ${_FULLDEST}\")
#      configure_file ( \"${_FULLSRC}\" \"${_FULLDEST}\" COPY_ONLY )
#    else ()
#      message (STATUS \"Keeping:    ${_FULLDEST}\")
#    endif ()
#  ")
#
#endmacro ()

#
# Test for packages
#

# ROOT
find_package (Root REQUIRED)
include_directories (${Root_INCDIR})

# ApMon
if (NOT ApMon_DISABLED)

  find_package (ApMon)

  if (ApMon_FOUND)
    include_directories (${ApMon_INCDIR})
    set (WITH_APMON TRUE)
  endif ()

endif ()

#
# Variables for the main destinations of "install" target, and for substitutions
# in configuration files. Typical directories are: etc, bin, libexec and var.
#

if (ROOT_MODE)
  message (STATUS "Compiling from within ROOT")
  set (DIR_ETC     "${ROOTSYS}/etc/proof")
  set (DIR_BIN     "${ROOTSYS}/bin")
  set (DIR_LIBEXEC "${ROOTSYS}/etc/proof")
  set (DIR_VAR     "${ROOTSYS}/etc/proof/var")
else ()
  set (DIR_ETC     "${CMAKE_INSTALL_PREFIX}/etc")
  set (DIR_BIN     "${CMAKE_INSTALL_PREFIX}/bin")
  set (DIR_LIBEXEC "${CMAKE_INSTALL_PREFIX}/libexec")
  set (DIR_VAR     "${CMAKE_INSTALL_PREFIX}/var")
endif ()

message (STATUS "Directory \"etc\" is ${DIR_ETC}")
message (STATUS "Directory \"bin\" is ${DIR_BIN}")
message (STATUS "Directory \"libexec\" is ${DIR_LIBEXEC}")
message (STATUS "Directory \"var\" is ${DIR_VAR}")

#
# Imports subdirectories
#

add_subdirectory (src)

#
# Auto-generate the configuration files with proper variables
#

# Several defines as a header file to be included
configure_file (
  "${PROJECT_SOURCE_DIR}/src/afOptions.h.in"
  "${PROJECT_BINARY_DIR}/src/afOptions.h"
  @ONLY
)

# Startup variables for the daemon
configure_file (
  "${PROJECT_SOURCE_DIR}/etc/sysconfig/afdsmgrd.example.in"
  "${PROJECT_BINARY_DIR}/etc/sysconfig/afdsmgrd.example"
  @ONLY
)

# Control script for the daemon
configure_file (
  "${PROJECT_SOURCE_DIR}/etc/init.d/afdsmgrd.in"
  "${PROJECT_BINARY_DIR}/etc/init.d/afdsmgrd"
  @ONLY
)

# Configuration file
configure_file (
  "${PROJECT_SOURCE_DIR}/etc/afdsmgrd.conf.example.in"
  "${PROJECT_BINARY_DIR}/etc/afdsmgrd.conf.example"
  @ONLY
)

# Sets the environment for ROOT defined in sysconfig script and launches it
configure_file (
  "${PROJECT_SOURCE_DIR}/scripts/afdsmgrd-root.sh.in"
  "${PROJECT_BINARY_DIR}/scripts/afdsmgrd-root.sh"
  @ONLY
)

#
# Install directives for non-src files
#

install (FILES ${CMAKE_CURRENT_BINARY_DIR}/etc/afdsmgrd.conf.example DESTINATION ${DIR_ETC})
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/etc/sysconfig/afdsmgrd.example DESTINATION ${DIR_ETC}/sysconfig)
install (PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/etc/init.d/afdsmgrd DESTINATION ${DIR_ETC}/init.d)
install (FILES macros/StageXrd.C DESTINATION ${DIR_LIBEXEC}/afdsmgrd-macros)
install (PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/scripts/afdsmgrd-root.sh DESTINATION ${DIR_LIBEXEC})
