import re

def replace_non_alphanum(val, replacement="_"):
    return re.sub('[^0-9a-zA-Z]+', replacement, val)

def convert_to_statsd_format(metric):
    """
    Format data for statsd topic
    
    Args:
      metric (dict): metric data with three required fields
          - timestamp - milliseconds since epoch
          - name_list - list of names that will make up the final metric name
          - type - metric type in set ('gauge', 'counter')
          - value - metric value
          - requires timestamp, type, value fields along with all name_keys

    Returns:
      Data for statsd topic with keys: timestamp, name, value, type

    """
    name = ".".join([replace_non_alphanum(n) for n in metric['name_list']])
    return { "timestamp": metric["timestamp"], "name": name, "value" : metric["value"], "type" : metric["type"]}
