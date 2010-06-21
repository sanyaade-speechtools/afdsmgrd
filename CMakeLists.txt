#
# Main CMake configuration file for afdsmgrd
#
# by Dario Berzano <dario.berzano@gmail.com>
#

# Name of the project and minimum version required
project (afdsmgrd)
cmake_minimum_required (VERSION 2.6)

# Where to find additional modules
set (CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules")

# The build type
set (CMAKE_BUILD_TYPE "Release")

# Tells the user the installation path
message (STATUS "Installation prefix: ${CMAKE_INSTALL_PREFIX}")

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
find_package(ApMon REQUIRED)
include_directories(${ApMon_INCDIR})

#
# Imports subdirectories
#

add_subdirectory (src)

#
# Auto-generate the configuration files with proper variables
#

configure_file (
  "${PROJECT_SOURCE_DIR}/etc/sysconfig/afdsmgrd.in"
  "${PROJECT_BINARY_DIR}/etc/sysconfig/afdsmgrd"
)

#
# Install directives for non-src files
#

install (FILES etc/xrootd/afdsmgrd.cf.example DESTINATION etc/xrootd)
install_dont_overwrite (${CMAKE_CURRENT_BINARY_DIR}/etc/sysconfig/afdsmgrd etc/sysconfig)
install (FILES etc/init.d/afdsmgrd DESTINATION etc/init.d PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
