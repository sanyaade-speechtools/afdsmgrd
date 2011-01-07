/**
 * afUrlRegex.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * See header file for a description of the class.
 */

#include "afUrlRegex.h"

using namespace af;

/** Constructor.
 */
urlRegex::urlRegex() : re_match(NULL), re_subst(NULL), sub_ptn(NULL) {}

/** Matches the given string to the previously set regular expression. If match
 *  succeeds it returns true; false if it fails. If the matching regex is not
 *  set, it always matches. If the given string is NULL, false is returned.
 */
bool urlRegex::match(const char *str) {

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

/** Sets and compiles the extended regular expression used to match strings. If
 *  there is an error in the regex it returns false and the former match regex
 *  is left intact; if everything went right, true is returned.
 */
bool urlRegex::set_regex_match(const char *ptn) {

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
void urlRegex::unset_regex_match() {
  if (re_match) {
    regfree(re_match);
    delete re_match;
    re_match = NULL;
  }
}

/** Sets and compiles the extended regular expression used for substitutions. If
 *  there is an error in the regex it returns false and the former substitution
 *  regex is left intact; if everything went right, true is returned.
 */
bool urlRegex::set_regex_subst(const char *ptn, const char *_sub_ptn) {

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
void urlRegex::unset_regex_subst() {
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
 *  regex was given or the original string is null, returns NULL.
 *
 *  This function returns a pointer to an internal static buffer: copy it
 *  before reusing member functions of this class again.
 *
 *  If the original string does not match the regular expression, NULL is
 *  returned too.
 */
const char *urlRegex::subst(const char *orig_str) {

  if ((re_subst == NULL) || (orig_str == NULL)) return NULL;

  // Match
  static const int n_match = 10;
  regmatch_t match[n_match];

  // See: http://www.gnu.org/s/libc/manual/html_node/
  // Matching-POSIX-Regexps.html#Matching-POSIX-Regexps
  if (regexec(re_subst, orig_str, n_match, match, 0) != 0) return NULL;

  // Substitute: assemble stuff (memory safe)
  strncpy(strbuf, sub_parts[0].ptr, AF_URLREGEX_BUFSIZE);
  size_t left = AF_URLREGEX_BUFSIZE-strlen(strbuf);
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

/** Test function (will be removed).
 */
void urlRegex::test() {

  regex_t re;
  int comp_status;
  const char *ptn = "^(alien)://(/.*)\\.root$|^(myproto)://(/.*)\\.root$";
  char subst_ptn[] = "root://$2$4_root.7z?proto=$1$3";

  // See: http://www.gnu.org/s/libc/manual/html_node/
  // Flags-for-POSIX-Regexps.html#Flags-for-POSIX-Regexps
  comp_status = regcomp(&re, ptn, REG_EXTENDED);

  printf("re=%s\n", ptn);
  printf("comp_status=%d\n", comp_status);

  if (comp_status == 0) {

    int exec_status;
    //std::string test_str = "alien:///alice/sim/2011/foo_bar.root";
    std::string test_str = "myproto:///alice/sim/2011/foo_bar.root";

    const int n_match = 20;
    regmatch_t match[n_match];

    // See: http://www.gnu.org/s/libc/manual/html_node/
    // Matching-POSIX-Regexps.html#Matching-POSIX-Regexps
    exec_status = regexec(&re, test_str.c_str(), n_match, match, 0);
    printf("test_str=%s\n", test_str.c_str());

    // Regexs that are not compiled successfully do not need to be freed
    regfree(&re);

    if (exec_status == 0) {

      printf("matches=true\n");
      for (int i=0; i<n_match; i++) {

        if (match[i].rm_so == -1) continue;

        printf("match[%d] --> %d,%d --> %s\n", i, match[i].rm_so,
          match[i].rm_eo, test_str.substr(match[i].rm_so,
          match[i].rm_eo-match[i].rm_so).c_str());
        printf("match[%d] --> ", i);

        for (int j=0; j<match[i].rm_so; j++)
          std::cout << test_str[j];

        std::cout << "\033[1;31m";
        for (int j=match[i].rm_so; j<match[i].rm_eo; j++)
          std::cout << test_str[j];
        std::cout << "\033[m";

        for (int j=match[i].rm_eo; test_str[j]!='\0'; j++)
          std::cout << test_str[j];

        std::cout << std::endl;
      }

      // Substitution

      // A submatch_t represents a couple <dollar#>[string]
      subparts_t parts;

      // A submatch_t.index = 0 means no dollar subst is performed there
      submatch_t cur_part;
      cur_part.index = 0;
      cur_part.ptr = subst_ptn;
      parts.push_back(cur_part);

      for (char *ptr = subst_ptn; *ptr!='\0'; ptr++) {
        if (ptr[0] == '$') {
          if ((ptr[1] >= '0') && (ptr[1] <= '9')) {

            cur_part.index = ptr[1] - '0';
            cur_part.ptr = &ptr[2];
            parts.push_back(cur_part);

            *ptr = '\0';

            ptr++;

          }
        }
      }

      // Print out parts
      /*for (unsigned int i=0; i<count_parts; i++) {
        printf("<dollar%u>{%s}", parts[i].index, parts[i].ptr);
      }
      std::cout << std::endl;*/

      // Assemble stuff (memory safe)
      const char *src = test_str.c_str();
      strncpy(strbuf, parts[0].ptr, AF_URLREGEX_BUFSIZE);
      size_t left = AF_URLREGEX_BUFSIZE-strlen(strbuf);
      for (unsigned int i=1; i<parts.size(); i++) {

        if (parts[i].index >= n_match) {
          strncat(strbuf, "<?>", left);
          left -= 3;
        }
        else {
          regmatch_t *m = &match[parts[i].index];
          size_t part_len;

          /*if (m->rm_eo < 0) {
            strncat(strbuf, "<!>", left);
            left -= 3;
          }
          else {*/
          if (m->rm_eo >= 0) {

            part_len = m->rm_eo-m->rm_so;
            if (part_len >= left) break;
            strncat(strbuf, &src[m->rm_so], part_len);
            left -= part_len;

          }
        }

        strncat(strbuf, parts[i].ptr, left);

      }

      printf("{%s}\n", strbuf);

    }
    else if (exec_status == REG_NOMATCH) printf("matches=false\n");
    else throw new std::runtime_error("Not enough memory to exec regex!");

  }

}

/** Destructor.
 */
urlRegex::~urlRegex() {
}
