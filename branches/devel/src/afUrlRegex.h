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
#include <vector>
#include <stdexcept>

#include <stdio.h>
#include <regex.h>
#include <string.h>
#include <stdlib.h>

namespace af {

  typedef struct {
    char *ptr;
    unsigned char index;
  } submatch_t;

  typedef std::vector<submatch_t> subparts_t;

  class urlRegex {

    public:

      urlRegex();
      virtual ~urlRegex();
      bool set_regex_match(const char *ptn);
      void unset_regex_match();
      void unset_regex_subst();
      bool match(const char *str);
      bool set_regex_subst(const char *ptn, const char *_sub_ptn);
      const char *subst(const char *orig_str);
      void test();

    private:

      char strbuf[AF_URLREGEX_BUFSIZE];
      regex_t *re_match;
      regex_t *re_subst;
      char *sub_ptn;
      subparts_t sub_parts;

  };

};

#endif // AFURLREGEX_H
