/**
 * afRegex.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * A wrapper around POSIX regex.h to match and substitute through the usage of
 * extended regular expressions.
 */

#ifndef AFREGEX_H
#define AFREGEX_H

#define AF_REGEX_BUFSIZE 2000

#include "afLog.h"

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

  class regex {

    public:

      regex();
      virtual ~regex();
      bool set_regex_match(const char *ptn);
      void unset_regex_match();
      void unset_regex_subst();
      bool match(const char *str);
      bool set_regex_subst(const char *ptn, const char *_sub_ptn);
      static std::string *dollar_subst(const char *ptn, unsigned int n_vars,
        const char **var_names, const char **var_values);
      const char *subst(const char *orig_str);
      void test();

    private:

      char strbuf[AF_REGEX_BUFSIZE];
      regex_t *re_match;
      regex_t *re_subst;
      char *sub_ptn;
      subparts_t sub_parts;

  };

};

#endif // AFREGEX_H
