import sys
import logging
import os
import warnings
import threading
from os import getenv
from six import add_metaclass

from botocore.session import Session

from pynamodb.constants import (
    REGION, CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS, BASE_BACKOFF_MS, MAX_RETRY_ATTEMPTS,
    MAX_POOL_CONNECTIONS, ALLOW_RATE_LIMITED_SCAN_WITHOUT_CONSUMED_CAPACITY, OBSOLETE_META_ATTRIBUTES
)

DEFAULT_SETTINGS = {
    CONNECT_TIMEOUT_SECONDS: 15,
    READ_TIMEOUT_SECONDS: 30,
    MAX_RETRY_ATTEMPTS: 3,
    MAX_POOL_CONNECTIONS: 10,
    BASE_BACKOFF_MS: 25,
    REGION: 'us-east-1',
    ALLOW_RATE_LIMITED_SCAN_WITHOUT_CONSUMED_CAPACITY: False
}


def _load_module(name, path):
    """Load module using the Python version compatible function."""
    if sys.version_info >= (3, 3):
        from importlib.machinery import SourceFileLoader
        return SourceFileLoader(name, path).load_module()
    else:
        from imp import load_source
        return load_source(name, path)


class SettingsMeta(type):
    """
    Settings meta class

    Implements a default Settings singleton to avoid multiple Settings instantiation.

    Note that the instantiation will be invoked at runtime
    but this implementation is thread-safe.
    """

    DEFAULT = 'default'

    def __init__(self, name, bases, attrs):
        super(SettingsMeta, self).__init__(name, bases, attrs)
        self._settings = None
        self._lock = threading.Lock()

    def __getattr__(self, key):
        if key == self.DEFAULT:
            # Fast path if already instantiated
            settings = self._settings
            if settings:
                return settings

            with self._lock:
                if not self._settings:
                    self._settings = Settings()
                return self._settings
        return super(SettingsMeta, self).__getattr__(key)

    def __setattr__(self, key, value):
        if key == self.DEFAULT:
            if not isinstance(value, Settings):
                raise ValueError("'{0}' must be an instance of Settings".format(self.DEFAULT))
            with self._lock:
                self._settings = value
        else:
            super(SettingsMeta, self).__setattr__(key, value)


@add_metaclass(SettingsMeta)
class Settings(object):
    """
    A class for accessing settings values and create botocore sessions.
    """

    def __init__(self, settings_path=None):
        super(Settings, self).__init__()
        log = logging.getLogger(__name__)
        if not settings_path:
            settings_path = getenv('PYNAMODB_CONFIG', '/etc/pynamodb/global_default_settings.py')
        if os.path.isfile(settings_path):
            self._override_settings = _load_module('__pynamodb_override_settings__', settings_path)
            for meta_attr in OBSOLETE_META_ATTRIBUTES:
                if hasattr(self._override_settings, meta_attr):
                    warnings.warn("The `%s` option is no longer supported" % meta_attr)
            log.info('Override settings for pynamo available {0}'.format(settings_path))
        else:
            self._override_settings = None
            log.info('Override settings for pynamo not available {0}'.format(settings_path))
            log.info('Using Default settings value')

    def __getattr__(self, key):
        """
        Fetches the value from the override file.
        If the value is not present, then fetch the values from default dictionary.
        """
        if self._override_settings and hasattr(self._override_settings, key):
            return getattr(self._override_settings, key)

        if key in DEFAULT_SETTINGS:
            return DEFAULT_SETTINGS[key]

        return None

    def get_session(self):
        """
        Create a new botocore Session

        :rtype: botocore.session.Session
        :return: Returns a botocore Session with default settings
        """
        return Session()
