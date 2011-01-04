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
add_library (afLog afLog.cc)
add_library (sqlite3 sqlite3.c)

target_link_libraries(afOpQueue afExtCmd sqlite3 -ldl -pthread)

#
# Classes which need a dictionary
#

#root_generate_dictionaries ( AfDict AfStageUrl.h AfDataSetSrc.h )

#
# List of executables and their link-time dependencies
#

#add_executable (afdsmgrd afdsmgrd.cc)
#target_link_libraries(afdsmgrd AfDataSetsManager AfConfReader AfDataSetSrc AfLog AfStageUrl AfDict ${Root_LIBS} ${ApMon_LIBRARY})

add_executable (afdsmgrd-exec-wrapper execwrapper.cc)

add_executable (readds readds.cc)
target_link_libraries (readds afDataSetList afOpQueue afConfig afLog ${Root_LIBS})


#
# Where to install the stuff
#

#install (TARGETS afdsmgrd RUNTIME DESTINATION bin)
