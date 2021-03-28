#!/usr/bin/env python

# Copyright (C) 2020 Intel Corporation
#
# SPDX-License-Identifier: MIT

from cvat.settings.production import *


# https://github.com/pennersr/django-allauth
ACCOUNT_AUTHENTICATION_METHOD = 'username'
ACCOUNT_CONFIRM_EMAIL_ON_GET = False
ACCOUNT_EMAIL_REQUIRED = False
ACCOUNT_EMAIL_VERIFICATION = 'none'

# Email backend settings for Django
EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend'
