/**
 * chdsacl.cc -- by Dario Berzano <dario.berzano@cern.ch>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * Standalone program to change permissions of a dataset to something writable
 * by afdsmgrd. It is meant to be run as root by setting the suid bit.
 */
 
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/stat.h>

/**
 */
char *secure_path(char *s) {
  char *p;
  if (!s) return NULL;
  for (p=s; p!='\0'; p++) {
    if (
      (*p < 'a') && (*p > 'z') &&
      (*p < 'A') && (*p > 'Z') &&
      (*p < '0') && (*p > '9') &&
      (*p != '_') && (*p != '-')
    ) *p = '_';
  }
  return s;
}

/** Entry point.
 */
int main(int argc, char *argv[]) {

  int c;

  opterr = 0;  // getopt lib: do not show standard errors

  const char *dsrepo = NULL;

  char *dsname = NULL;
  char *dsuser = NULL;
  char *dsgroup = NULL;

  const char *user = NULL;
  const char *group = NULL;

  while ((c = getopt(argc, argv, "+:n:r:u:g:U:G:")) != -1) {
    switch (c) {
      case 'n':
        dsname = optarg;
      break;

      case 'r':
        dsrepo = optarg;
      break;

      case 'u':
        dsuser = optarg;
      break;

      case 'g':
        dsgroup = optarg;
      break;

      case 'U':
        user = optarg;
      break;

      case 'G':
        group = optarg;
      break;

      case ':':
        printf("Option '-%c' requires an argument\n", optopt);
        return 1;
      break;

      case '?':
        printf("Unknown option: '-%c'\n", optopt);
        return 2;
      break;
    }
  }

  // Check if output files were given
  bool missing_args = false;

  if ((!dsname) || (!dsuser) || (!dsgroup) || (!dsrepo) ||
      (!user) || (!group)) {
    fprintf(stderr, "Wrong arguments.\n");
    fprintf(stderr, "Usage: %s -r <dsrepo> -U <unix_user> -G <unix_grp> \\\n",
      argv[0]);
    fprintf(stderr, "  -n <dsname> -g <dsgroup> -u <dsuser>\n");
    return 3;
  }

  mode_t rwx = S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IXGRP|S_IROTH|\
    S_IWOTH|S_IXOTH;
  mode_t rw = S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH;

  std::string path = dsrepo;

  // Repository
  if (chmod(path.c_str(), rwx) != 0) return 4;

  // Group
  path += '/';
  path += secure_path(dsgroup);
  //puts(path.c_str());
  if (chmod(path.c_str(), rwx) != 0) return 4;

  // User
  path += '/';
  path += secure_path(dsuser);
  //puts(path.c_str());
  if (chmod(path.c_str(), rwx) != 0) return 4;

  // Dataset name
  path += '/';
  path += secure_path(dsname);
  path += ".root";
  //puts(path.c_str());
  if (chmod(path.c_str(), rw) != 0) return 4;

  return 0;
}
