#include <stdio.h>

extern "C" {

  void hello_init() {
    printf("hello_1 plugin inited\n");
  }

}
