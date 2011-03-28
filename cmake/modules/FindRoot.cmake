#
# Cmake module to search for a ROOT installation. It only requires the ROOTSYS
# envvar to be set.
#
# by Dario Berzano <dario.berzano@gmail.com>
#

#
# Search for the ROOTSYS envvar
#

set (ROOTSYS $ENV{ROOTSYS})
if (ROOTSYS)
  get_filename_component (ROOTSYS ${ROOTSYS} ABSOLUTE)
  message (STATUS "[ROOT] ROOTSYS (from environment): ${ROOTSYS}")
  set (ROOTSYS ${ROOTSYS} CACHE PATH "The root of the ROOT installation };-)")
else ()
  if (Root_FIND_REQUIRED)
    message (FATAL_ERROR "ROOTSYS is not set!")
  endif ()
endif ()

#
# Search for root-config
#

find_program(Root_CONFIG
  NAMES root-config
  PATHS ${ROOTSYS}/bin
)

if (NOT Root_CONFIG)
  set (Root_FOUND FALSE)
  if (Root_FIND_REQUIRED)
    message (FATAL_ERROR "root-config not found, is ROOT installed in ${ROOTSYS}?")
  endif ()
else ()
  message (STATUS "[ROOT] root-config path: ${Root_CONFIG}")
endif ()

#
# Search for rootcint
#

find_program(Root_ROOTCINT
  NAMES rootcint
  PATHS ${ROOTSYS}/bin
)

if (NOT Root_ROOTCINT)
  set (Root_FOUND FALSE)
  if (Root_FIND_REQUIRED)
    message (FATAL_ERROR "rootcint not found, is ROOT installation corrupted?")
  endif ()
else ()
  message (STATUS "[ROOT] rootcint path: ${Root_ROOTCINT}")
endif ()

#
# Some ROOT build options
#

#message(STATUS "Looking for ROOT build options...")

execute_process(COMMAND ${Root_CONFIG} --libs OUTPUT_VARIABLE Root_LIBS OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${Root_CONFIG} --libdir OUTPUT_VARIABLE Root_LIBDIR OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${Root_CONFIG} --incdir OUTPUT_VARIABLE Root_INCDIR OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${Root_CONFIG} --cflags OUTPUT_VARIABLE Root_CFLAGS OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${Root_CONFIG} --version OUTPUT_VARIABLE Root_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)

message (STATUS "[ROOT] Version: ${Root_VERSION}")
message (STATUS "[ROOT] Include path: ${Root_INCDIR}")
message (STATUS "[ROOT] Library path: ${Root_LIBDIR}")

#separate_arguments (Root_LIBS)

# Append some ROOT extra libraries not given by --libs
set (Root_LIBS ${Root_LIBS} -lProof)

#
# In the end, was ROOT found?
#

if (NOT Root_FOUND)
  set (Root_FOUND TRUE)
endif ()

#
# A macro that uses rootcint to generate dictionaries for ROOT classes.
#
# The first argument is the final library that will hold the dictionaries, the
# other arguments are the header files of the classes that need a dictionary.
#

macro (root_generate_dictionaries)

  set (_INHEADERS ${ARGV})
  list (GET _INHEADERS 0 _FINALDICT)
  list (REMOVE_AT _INHEADERS 0)

  foreach (_INHEADER ${_INHEADERS})

    get_filename_component (_CLASS_BASE_NAME ${_INHEADER} NAME_WE)
    set (_OUTDICT ${_CLASS_BASE_NAME}_dict.cc)
    set (_OUTDICT_HEADER ${_CLASS_BASE_NAME}_dict.h)

    add_custom_command (
      OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${_OUTDICT} ${CMAKE_CURRENT_BINARY_DIR}/${_OUTDICT_HEADER}
      COMMAND LD_LIBRARY_PATH=${Root_LIBDIR}:$ENV{LD_LIBRARY_PATH} ${ROOTSYS}/bin/rootcint -f ${CMAKE_CURRENT_BINARY_DIR}/${_OUTDICT} -c -I${Root_INCDIR} -p ${CMAKE_CURRENT_SOURCE_DIR}/${_INHEADER}
      DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${_INHEADER}
      IMPLICIT_DEPENDS CXX ${CMAKE_CURRENT_SOURCE_DIR}/${_INHEADER}
      COMMENT "Generating dictionary for class ${_CLASS_BASE_NAME}"
    )

    list (APPEND _ROOT_DICTS ${_OUTDICT})

  endforeach ()

  # This command adds at once the dictionaries to a single static library
  add_library(${_FINALDICT} ${_ROOT_DICTS})

endmacro ()
