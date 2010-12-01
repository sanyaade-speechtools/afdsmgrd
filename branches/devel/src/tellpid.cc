/**
 */
#include <stdio.h>
#include <sys/types.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>

#include "sqlite3.h"

int main(int argn, char *argv[]) {

  pid_t process_pid;

  if ((argn < 2) || ((process_pid = atoi(argv[1])) == 0)) {
    printf("Usage: %s <pid>\n", argv[0]);
    return 1;
  }

  while (1) {
    if (kill(process_pid, 0) == -1) {
      printf("process %d terminated\n", process_pid);
      break;
    }
    else {
      printf("process %d running\n", process_pid);
      sleep(1);
    }
  }

  return 0;
}
