/**
 * afRegex.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * See header file for a description of the class.
 */

#include "afRegex.h"

using namespace af;

/** Constructor.
 */
regex::regex() : re_match(NULL), re_subst(NULL), sub_ptn(NULL) {}

/** Destructor.
 */
regex::~regex() {
  unset_regex_match();
  unset_regex_subst();
}

/** Sets and compiles the extended regular expression used to match strings. If
 *  there is an error in the regex it returns false and the former match regex
 *  is left intact; if everything went right, true is returned.
 */
bool regex::set_regex_match(const char *ptn) {

  if (ptn == NULL) return false;

  regex_t *re_compd = new regex_t;

  // See: http://www.gnu.org/s/libc/manual/html_node/
  // Flags-for-POSIX-Regexps.html#Flags-for-POSIX-Regexps
  if (regcomp(re_compd, ptn, REG_EXTENDED|REG_NOSUB) != 0) {
    delete re_compd;
    return false;
  }
  else {
    unset_regex_match();
    re_match = re_compd;
  }

  return true;
}

/** Frees the space used by the match regex. Safe to call if match regex was not
 *  previously set (it does nothing in such a case).
 */
void regex::unset_regex_match() {
  if (re_match) {
    regfree(re_match);
    delete re_match;
    re_match = NULL;
  }
}

/** Matches the given string to the previously set regular expression. If match
 *  succeeds it returns true; false if it fails. If the matching regex is not
 *  set, it always matches. If the given string is NULL, false is returned.
 */
bool regex::match(const char *str) {

    if (str == NULL) return false;
    if (re_match == NULL) return true;

    // See: http://www.gnu.org/s/libc/manual/html_node/
    // Matching-POSIX-Regexps.html#Matching-POSIX-Regexps
    int exec_status = regexec(re_match, str, 0, NULL, 0);

    if (exec_status == 0) return true;
    else if (exec_status != REG_NOMATCH)
      throw new std::runtime_error("Not enough memory to perform regex!");

    return false;
}

/** Sets and compiles the extended regular expression used for substitutions. If
 *  there is an error in the regex it returns false and the former substitution
 *  regex is left intact; if everything went right, true is returned.
 */
bool regex::set_regex_subst(const char *ptn, const char *_sub_ptn) {

  if ((_sub_ptn == NULL) || (ptn == NULL)) return false;

  regex_t *re_compd = new regex_t;

  // See: http://www.gnu.org/s/libc/manual/html_node/
  // Flags-for-POSIX-Regexps.html#Flags-for-POSIX-Regexps
  if (regcomp(re_compd, ptn, REG_EXTENDED) != 0) {
    delete re_compd;
    return false;
  }
  else {
    unset_regex_subst();
    re_subst = re_compd;
  }

  // Prepare substitution pattern

  sub_ptn = strdup(_sub_ptn);
  if (sub_ptn == NULL)
    throw new std::runtime_error("Not enough memory to store pattern");

  submatch_t cur_part;
  cur_part.index = 0;
  cur_part.ptr = sub_ptn;
  sub_parts.push_back(cur_part);

  for (char *ptr = sub_ptn; *ptr!='\0'; ptr++) {
    if (ptr[0] == '$') {
      if ((ptr[1] >= '0') && (ptr[1] <= '9')) {
        cur_part.index = ptr[1] - '0';
        cur_part.ptr = &ptr[2];
        sub_parts.push_back(cur_part);
        *ptr = '\0';
        ptr++;
      }
    }
  }

  return true;
}

/** Frees the space used by the substitution regex and substitution pattern.
 *  Safe to call if substitution regex was not previously set (it does nothing
 *  in such a case).
 */
void regex::unset_regex_subst() {
  if (re_subst) {
    regfree(re_subst);
    delete re_subst;
    re_subst = NULL;
    free(sub_ptn);
    sub_ptn = NULL;
    sub_parts.clear();
  }
}

/** Builds a new string (returned) from the original string (passed) based on
 *  the previously entered substitution regex and pattern. If no substitution
 *  regex was given or the original string is NULL, returns NULL.
 *
 *  This function returns a pointer to an internal static buffer: copy it
 *  before reusing member functions of this class again.
 *
 *  If the original string does not match the regular expression, NULL is
 *  returned too.
 */
const char *regex::subst(const char *orig_str) {

  if ((re_subst == NULL) || (orig_str == NULL)) return NULL;

  // Match
  static const int n_match = 10;
  regmatch_t match[n_match];

  // See: http://www.gnu.org/s/libc/manual/html_node/
  // Matching-POSIX-Regexps.html#Matching-POSIX-Regexps
  if (regexec(re_subst, orig_str, n_match, match, 0) != 0) return NULL;

  // Substitute: assemble stuff (memory safe)
  strncpy(strbuf, sub_parts[0].ptr, AF_REGEX_BUFSIZE);
  size_t left = AF_REGEX_BUFSIZE-strlen(strbuf);
  for (unsigned int i=1; i<sub_parts.size(); i++) {

    if (sub_parts[i].index >= n_match) {
      strncat(strbuf, "<?>", left);
      left -= 3;
    }
    else {
      regmatch_t *m = &match[sub_parts[i].index];
      size_t part_len;
      if (m->rm_eo >= 0) {
        part_len = m->rm_eo-m->rm_so;
        if (part_len >= left) break;
        strncat(strbuf, &orig_str[m->rm_so], part_len);
        left -= part_len;
      }
    }

    strncat(strbuf, sub_parts[i].ptr, left);
  }

  return strbuf;
}
