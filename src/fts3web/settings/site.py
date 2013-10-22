# Copyright notice:
# Copyright (C) Members of the EMI Collaboration, 2010.
#
# See www.eu-emi.eu for details on the copyright holders
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from common import BASE_URL, FTS3WEB_CONFIG
import os
import urlparse

def _urlize(path):
    url = urlparse.urlparse(path)
    if url.scheme:
        return path
    else:
        return path % {'base': BASE_URL} 

SITE_NAME       = FTS3WEB_CONFIG.get('site', 'name')
SITE_LOGO       = _urlize(FTS3WEB_CONFIG.get('site', 'logo'))
SITE_LOGO_SMALL = _urlize(FTS3WEB_CONFIG.get('site', 'logo_small'))

ADMINS = (
    (FTS3WEB_CONFIG.get('site', 'admin_name'), FTS3WEB_CONFIG.get('site', 'admin_mail'))
)

MANAGERS = ADMINS

if FTS3WEB_CONFIG.get('logs', 'port'):
    LOG_BASE_URL =  "%s://%%(host):%d/%s" % (FTS3WEB_CONFIG.get('logs', 'scheme'),
                                             FTS3WEB_CONFIG.getint('logs', 'port'),
                                             FTS3WEB_CONFIG.get('logs', 'base'))
else:
    LOG_BASE_URL =  "%s://%%(host)/%s" % (FTS3WEB_CONFIG.get('logs', 'scheme'),
                                          FTS3WEB_CONFIG.get('logs', 'base'))

