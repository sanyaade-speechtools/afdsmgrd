#
# CMake module to search for ApMon (MonALISA client library) installation
#
# by Dario Berzano <dario.berzano@gmail.com>
#

#
# Search for the ApMon_PREFIX variable
#

set (ALIEN_DIR $ENV{ALIEN_DIR})

find_path (ApMon_INCDIR
  ApMon.h
  PATHS ${ApMon_PREFIX}/include ${ALIEN_DIR}/api/include ${ALIEN_DIR}/include
)

find_library (ApMon_LIBRARY
  apmoncpp
  PATHS ${ApMon_PREFIX}/lib ${ALIEN_DIR}/api/lib ${ALIEN_DIR}/api/lib64 ${ALIEN_DIR}/lib ${ALIEN_DIR}/lib64
)

if ((NOT ApMon_INCDIR) OR (NOT ApMon_LIBRARY))
  set (ApMon_FOUND FALSE)
  if (ApMon_FIND_REQUIRED)
    message (FATAL_ERROR "ApMon not found!")
  else ()
    message (STATUS "ApMon not found")
  endif ()
else ()
  message (STATUS "[ApMon] Include path: ${ApMon_INCDIR}")
  message (STATUS "[ApMon] Dynamic library: ${ApMon_LIBRARY}")
  set (ApMon_FOUND TRUE)
endif ()
