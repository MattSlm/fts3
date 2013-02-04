/* Copyright @ Members of the EMI Collaboration, 2010.
See www.eu-emi.eu for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License"); 
you may not use this file except in compliance with the License. 
You may obtain a copy of the License at 

    http://www.apache.org/licenses/LICENSE-2.0 

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" BASIS, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
See the License for the specific language governing permissions and 
limitations under the License. */

#include "name_to_uid.h"

/*courtesy of:
"Setuid Demystified" by Hao Chen, David Wagner, and Drew Dean: http://www.cs.berkeley.edu/~daw/papers/setuid-usenix02.pdf
 */
uid_t name_to_uid() {
    char name[ ] = "fts3";
    long const buflen = sysconf(_SC_GETPW_R_SIZE_MAX);
    if (buflen == -1)
        return static_cast<uid_t>(-1);

    char buf[buflen];
    struct passwd pwbuf, *pwbufp;
    if (0 != getpwnam_r(name, &pwbuf, buf, static_cast<size_t>(buflen), &pwbufp)
            || !pwbufp)
        return static_cast<uid_t>(-1);
    return pwbufp->pw_uid;
}

