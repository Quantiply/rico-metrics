
class RicoMetrics(object):

    def __init__(self, sys_conf_path=DEFAULT_SYS_CONFIG_PATH):
        self._sys_conf_path = sys_conf_path
        
    def to_statsd(self, msg):
        