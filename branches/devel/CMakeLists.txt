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
# asdfghjkl
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
# A handy macro to copy a file in install phase without overwriting it
#

macro (install_dont_overwrite SRCFILE DESTDIR)

  get_filename_component (_FILENAME ${SRCFILE} NAME)
  get_filename_component (_FULLSRC ${SRCFILE} ABSOLUTE)
  get_filename_component (_FULLDEST ${CMAKE_INSTALL_PREFIX}/${DESTDIR}/${_FILENAME} ABSOLUTE)

  install (CODE "
    if (NOT EXISTS ${_FULLDEST})
      message (STATUS \"Installing: ${_FULLDEST}\")
      configure_file ( \"${_FULLSRC}\" \"${_FULLDEST}\" COPY_ONLY )
    else ()
      message (STATUS \"Keeping:    ${_FULLDEST}\")
    endif ()
  ")

endmacro ()

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
# Imports subdirectories
#

add_subdirectory (src)

#
# Auto-generate the configuration files with proper variables
#

configure_file (
  "${PROJECT_SOURCE_DIR}/src/afOptions.h.in"
  "${PROJECT_BINARY_DIR}/src/afOptions.h"
  @ONLY
)

configure_file (
  "${PROJECT_SOURCE_DIR}/etc/sysconfig/afdsmgrd.in"
  "${PROJECT_BINARY_DIR}/etc/sysconfig/afdsmgrd"
  @ONLY
)

configure_file (
  "${PROJECT_SOURCE_DIR}/etc/init.d/afdsmgrd.in"
  "${PROJECT_BINARY_DIR}/etc/init.d/afdsmgrd"
  @ONLY
)

configure_file (
  "${PROJECT_SOURCE_DIR}/etc/afdsmgrd.conf.example.in"
  "${PROJECT_BINARY_DIR}/etc/afdsmgrd.conf.example"
  @ONLY
)

#
# Install directives for non-src files
#

install (FILES ${CMAKE_CURRENT_BINARY_DIR}/etc/afdsmgrd.conf.example DESTINATION etc)
install_dont_overwrite (${CMAKE_CURRENT_BINARY_DIR}/etc/sysconfig/afdsmgrd etc/sysconfig)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/etc/init.d/afdsmgrd DESTINATION etc/init.d PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
install (FILES macros/StageXrd.C DESTINATION libexec/afdsmgrd-macros)
