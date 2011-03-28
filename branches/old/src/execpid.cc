// Standard includes
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <cstdio>

using namespace std;

int main(int argc, char *argv[]) {

  int c;

  opterr = 0;  // getopt lib: do not show standard errors

  const char *pidFn = NULL;

  while ((c = getopt(argc, argv, "+:p:")) != -1) {
    switch (c) {
      case 'p':
        pidFn = optarg;
      break;

      case ':':
        printf("Option '-%c' requires an argument\n", optopt);
        return 1;
      break;

      case '?':
        printf("Unknown option: '-%c'\n", optopt);
        return 2;
      break;
    } // switch
  } // while

  // Check if output files were given
  if (!pidFn) {
    printf("PID file must be specified\n");
    return 3;
  }

  if (optind == argc) {
    printf("Specify the command and its arguments to run after the other "
      "parameters\n");
    return 4;
  }

  // Assemble the command line
  ofstream pidFile(pidFn);
  if (pidFile) {
    pidFile << getpid() << endl;
    pidFile.close();
  }
  else {
    printf("Can't write on PID file %s", pidFn);
    return 5;
  }

  execvp(argv[optind], &argv[optind]);

  return 0;
}
