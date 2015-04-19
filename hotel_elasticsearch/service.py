import platform
from subprocess import Popen
import os


class ElasticSearchService(object):

    def _platform(self):
        if platform.system() == 'Darwin':
            return "OS X"
        elif platform.system() == 'Linux':
            return 'linux'
        return None

    def start(self):
        if self._platform() == 'OS X':
            Popen(['launchctl', 'load', os.path.expanduser('~/Library/LaunchAgents/homebrew.mxcl.elasticsearch.plist')]).wait()
        elif self._platform() == 'linux':
            if Popen(['/etc/init.d/elasticsearch', 'start']).wait():
                raise Exception("Could not start elastic search")
        else:
            raise Exception("Cannot manage Elastic Search on this platform")

    def stop(self):
        if self._platform() == 'OS X':
            if Popen(['launchctl', 'unload', os.path.expanduser('~/Library/LaunchAgents/homebrew.mxcl.elasticsearch.plist')]).wait():
                raise Exception("Could not stop Elastic Search")

        elif self._platform() == 'linux':
            if Popen(['/etc/init.d/elasticsearch', 'start']).wait():
                raise Exception("Could not stop elastic search")
        else:
            raise Exception("Cannot manage Elastic Search on this platform")

    def running(self):
        if self._platform() == 'OS X':
            if Popen(['launchctl', 'list', 'homebrew.mxcl.elasticsearch']).wait() == 0:
                return True
            return False
        elif self._platform() == 'linux':
            if Popen(['/etc/init.d/elasticsearch', 'status']).wait() == 0:
                return True
            return False
        else:
            raise Exception("Cannot manage Elastic Search on this platform")