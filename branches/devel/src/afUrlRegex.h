/**
 * afUrlRegex.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * This class matches and translates URLs based on a given list of extended
 * regular expressions.
 */

#ifndef AFURLREGEX_H
#define AFURLREGEX_H

#define AF_URLREGEX_BUFSIZE 2000

#include <iostream>
#include <stdexcept>

#include <stdio.h>
#include <regex.h>
#include <string.h>

namespace af {

  typedef struct {
    char *ptr;
    unsigned char index;
  } submatch_t;

  class urlRegex {

    public:

      urlRegex();
      virtual ~urlRegex();

    private:

      char strbuf[AF_URLREGEX_BUFSIZE];

  };

};

#endif // AFURLREGEX_H
