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

/** \file config_dev.h Common FTS3 config component constructs. Each FTS3 config 
 * source files must include it. 
 */

#ifndef FTS3_CONFIG_DEV_H
#define FTS3_CONFIG_DEV_H

#include "common/dev.h"

#ifndef FTS3_CONFIG_NAMESPACE_START
  /** Start FTS3 config namespace. Example:
      \code
      FTS3_CONFIG_NAMESPACE_START
          code....
          ...
      FTS3_CONFIG_NAMESPACE_END
      \endcode
  */ 
  #define FTS3_CONFIG_NAMESPACE_START \
    FTS3_NAMESPACE_START \
    namespace config {
#endif

#ifndef FTS3_CONFIG_NAMESPACE_END
  /** End FTS3 config namespace. Example:
      \code
      FTS3_CONFIG_NAMESPACE_START
          code....
          ...
      FTS3_CONFIG_NAMESPACE_END
      \endcode
  */ 
  #define FTS3_CONFIG_NAMESPACE_END \
    FTS3_NAMESPACE_END } 
#endif

#ifndef FTS3_CONFIG_NAMESPACE
  /** Defines FTS3 config namespace name. */
  #define FTS3_CONFIG_NAMESPACE fts3::config
#endif

#endif // FTS3_CONFIG_DEV_H

