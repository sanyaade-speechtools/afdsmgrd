#include <stdio.h>

extern "C" {

  void hello_init() {
    printf("hello_2 plugin inited\n");
  }

}
