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
urlRegex::urlRegex() {

  regex_t re;
  int comp_status;
  const char *ptn = "^(alien)://(/.*)\\.root$";
  char subst_ptn[] = "root://$2_root.7z?proto=$1&cose=$3$3$3$1$1";

  // See: http://www.gnu.org/s/libc/manual/html_node/
  // Flags-for-POSIX-Regexps.html#Flags-for-POSIX-Regexps
  comp_status = regcomp(&re, ptn, REG_EXTENDED);

  printf("re=%s\n", ptn);
  printf("comp_status=%d\n", comp_status);

  if (comp_status == 0) {

    int exec_status;
    std::string test_str = "alien:///alice/sim/2011/foo_bar.root";

    const int n_match = 3;
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

        if (match[i].rm_so == -1) break;

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
      const unsigned int max_parts = 20;
      submatch_t parts[max_parts];
      memset(parts, 0, sizeof(parts));

      unsigned int count_parts = 1;

      // A submatch_t.index = 0 means no dollar subst is performed there
      parts[0].index = 0;
      parts[0].ptr = subst_ptn;

      for (char *ptr = subst_ptn; *ptr!='\0'; ptr++) {
        if (ptr[0] == '$') {
          if ((ptr[1] >= '0') && (ptr[1] <= '9')) {

            parts[count_parts].index = ptr[1] - '0';
            parts[count_parts].ptr = &ptr[2];

            *ptr = '\0';

            if (++count_parts == max_parts) break;

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
      for (unsigned int i=1; i<count_parts; i++) {

        if (parts[i].index >= n_match) {
          strncat(strbuf, "<?>", left);
          left -= 3;
        }
        else {
          regmatch_t *m = &match[parts[i].index];
          size_t part_len;

          if (m->rm_eo < 0) {
            strncat(strbuf, "<?>", left);
            left -= 3;
          }
          else {

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
