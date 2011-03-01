#
# A Cmake complete test that links to ROOT libraries and generates dictionaries.
#
# by Dario Berzano <dario.berzano@gmail.com>
#

#
# For out-of-source builds, include the destination dir *before* (default
# behavior) the others as the search base for includes
#

include_directories (${CMAKE_CURRENT_BINARY_DIR})

#
# List of the libraries to build
#

add_library (afDataSetList afDataSetList.cc)
add_library (afOpQueue afOpQueue.cc)
add_library (afExtCmd afExtCmd.cc)
add_library (afConfig afConfig.cc)
add_library (afRegex afRegex.cc)
add_library (afLog afLog.cc)
add_library (sqlite3 sqlite3.c)

add_library (shared_1 SHARED shared_1.cc)
add_library (shared_2 SHARED shared_2.cc)

target_link_libraries(afOpQueue afExtCmd sqlite3 -ldl -pthread)

#
# List of executables and their link-time dependencies
#

add_executable (afdsmgrd-exec-wrapper execwrapper.cc)

add_executable (readds readds.cc)
target_link_libraries (readds afDataSetList afOpQueue afConfig afLog afExtCmd afRegex ${Root_LIBS})

add_executable (afdsmgrd afdsmgrd.cc)
target_link_libraries (afdsmgrd afLog afConfig ${Root_LIBS})

#
# Where to install the stuff
#

install (TARGETS afdsmgrd RUNTIME DESTINATION bin)
