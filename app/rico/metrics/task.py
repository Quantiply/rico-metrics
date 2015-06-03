#
# Copyright 2014-2015 Quantiply Corporation. All rights reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
from com.quantiply.samza.task import BaseTask
from org.apache.samza.system import OutgoingMessageEnvelope
from rico.metrics.samza import SamzaMetricsConverter
import re
import statsd
import time 
import datetime
import traceback

def replace_non_alphanum(val, replacement="_"):
    return re.sub('[^0-9a-zA-Z]+', replacement, val)

def convert_to_statsd_format(name_keys, metric):
    """
    Format data for statsd topic
    
    Args:
      name_keys: List of keys for join to create the metric name
      metric: metric data
          - requires timestamp, type, value fields along with all name_keys

    Returns:
      Data for statsd topic with keys: timestamp, name, value, type

    """
    name = ".".join([replace_non_alphanum(metric[i]) for i in name_keys])
    return { "timestamp": metric["timestamp"], "name": name, "value" : metric["value"], "type" : metric["type"]}

class SamzaMetricsTask(BaseTask):
    converter = SamzaMetricsConverter()
  
    def _init(self, config, context, metric_adaptor):
        self.output = self.getSystemStream("out")
        self.registerDefaultHandler(self.handle_msg)

    def handle_msg(self, envelope, collector, coord):
        try:
            if envelope.message.has_key("asMap"): #For Samza 0.8.0 compatibility
                samza_metrics = envelope.message["asMap"]
            else:
                samza_metrics = envelope.message
            for metric in self.converter.get_statsd_metrics(samza_metrics):
                collector.send(OutgoingMessageEnvelope(self.output, metric))
        except Exception, e:
            self.logger.error(traceback.format_exc())
            if self.logger.isDebugEnabled:
                self.logger.debug("Message was: " + str(envelope))
            raise e

        
class DruidMetricsTask(BaseTask):
    
    def _init(self, config, context, metric_adaptor):
        self.output = self.getSystemStream("out")
        self.registerDefaultHandler(self.handle_msg)

    def handle_msg(self, data, collector, coord):
        try:
            msg = data.message
            PREFIXES = ['events', 'rows', 'failed', 'persist']
            if (any([msg["metric"].startswith(prefix) for prefix in PREFIXES])):
                data = {}
                data["source"] = "druid"
                data["metric"] = msg["metric"]
                data["service"] = msg["service"]
                data["host"] = msg["host"]
                data["timestamp"] = datetime.datetime.strptime(msg["timestamp"],\
                                                               "%Y-%m-%dT%H:%M:%S.%fZ")
                data["type"] = "counter"
                data["data_source"] = msg["user2"]
                data["value"] = msg["value"]
                names = ["source", "service", "host", "data_source", "metric"]
                collector.send(OutgoingMessageEnvelope(self.output, convert_to_statsd_format(names, data)))                
        except Exception, e:
            if (self.logger.isInfoEnabled):
                self.logger.info("Error while processing record" + str(data) + ": " + e.message)
            raise e

class StatsDTask(BaseTask):
    
    def _init(self, config, context, metric_adaptor):
        statsd_host = config.get("rico.statsd.host")
        statsd_port = config.get("rico.statsd.port")
        
        self.client = statsd.StatsClient(statsd_host, statsd_port)
        self.drop_secs = int(config.get("rico.drop.secs"))
        self.prefix = config.get("rico.statsd.prefix")
        self.logger.info("Drop secs: %s" % self.drop_secs)
        
        self.registerDefaultHandler(self.handle_msg)

    def handle_msg(self, data, collector, coord):
        try:
            msg = data.message
            name = "%s.%s" % (self.prefix, msg["name"] )
            timestamp = msg["timestamp"]
            metric_type = msg["type"]
            current_time_in_ms = int(round(time.time() * 1000))
            time_diff_in_secs = (current_time_in_ms - timestamp) / 1000
            # Check if the metric is within the window period
            if time_diff_in_secs > self.drop_secs:
                if self.logger.isDebugEnabled:
                    self.logger.debug("Time diff %ss is greater than configured maximum %ss...dropping msg" % (time_diff_in_secs, self.drop_secs))
                self.client.incr("samza.statsd_push.dropped_messages", 1)
            else:
                if self.logger.isDebugEnabled:
                    self.logger.debug("|".join([metric_type, name, str(msg['value'])]))
                if metric_type == "gauge":
                    self.client.gauge(name, msg["value"])
                elif metric_type == "counter":
                    self.client.incr(name, msg["value"])
        except Exception, e:
            if (self.logger.isInfoEnabled):
                self.logger.info("Error while processing record" + str(data) + ": " + e.message)
            raise e

