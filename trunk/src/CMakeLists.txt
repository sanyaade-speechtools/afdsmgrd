#
# A Cmake complete test that links to ROOT libraries and generates dictionaries.
#
# by Dario Berzano <dario.berzano@gmail.com>
#

#
# List of the libraries to build
#

add_library (AfConfReader AfConfReader.cc)
add_library (AfDataSetsManager AfDataSetsManager.cc)
add_library (AfDataSetSrc AfDataSetSrc.cc)
add_library (AfLog AfLog.cc)
add_library (AfStageUrl AfStageUrl.cc)

#
# Classes which need a dictionary
#

root_generate_dictionaries ( AfDict AfStageUrl.h AfDataSetSrc.h )

#
# List of executables and their link-time dependencies
#

add_executable (afdsmgrd afdsmgrd.cc)
target_link_libraries(afdsmgrd AfDataSetsManager AfConfReader AfDataSetSrc AfLog AfStageUrl AfDict ${Root_LIBS} ${ApMon_LIBRARY})

#
# Where to install the stuff
#

install (TARGETS afdsmgrd RUNTIME DESTINATION bin)
