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
import re
from statsd import TCPStatsClient, StatsClient
import time 
from datetime import datetime
import traceback
from com.quantiply.samza.task import BaseTask
from org.apache.samza.system import OutgoingMessageEnvelope
from rico.metrics.samza import SamzaMetricsConverter
from rico.metrics.statsd_util import convert_to_statsd_format

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
    DS_PREFIXES = ['events', 'rows', 'failed', 'persist', 'query']
    NODE_PREFIXES = ['exec', 'cache', 'jvm']
    
    def _init(self, config, context, metric_adaptor):
        self.output = self.getSystemStream("out")
        self.registerDefaultHandler(self.handle_msg)

    def handle_msg(self, data, collector, coord):
        try:
            msg = data.message
            if msg['feed'] == 'alerts':
                #druid.<node-type>.<node>.alerts.<severity>
                metric = {
                    "source": "druid",
                    "timestamp": datetime.strptime(msg["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                    "name_list": ['druid', msg["service"], msg["host"], 'alerts', msg['severity']],
                    "type": 'counter',
                    "value": 1
                }
                collector.send(OutgoingMessageEnvelope(self.output, convert_to_statsd_format(metric)))
            elif msg['feed'] == 'metrics':
              if any([msg["metric"].startswith(prefix) for prefix in self.DS_PREFIXES]):
                  #druid.<node-type>.<node>.datasource.<datasource>.<metric>
                  metric = {
                      "source": "druid",
                      "timestamp": datetime.strptime(msg["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                      "name_list": ['druid', msg["service"], msg["host"], 'datasource', msg["user2"]] + msg["metric"].split('/'),
                      "type": 'counter',
                      "value": msg["value"]
                  }
                  collector.send(OutgoingMessageEnvelope(self.output, convert_to_statsd_format(metric)))
              elif any([msg["metric"].startswith(prefix) for prefix in self.NODE_PREFIXES]):
                  #druid.<node-type>.<node>.node.<metric>
                  metric = {
                      "source": "druid",
                      "timestamp": datetime.strptime(msg["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                      "name_list": ['druid', msg["service"], msg["host"], 'node'] + msg["metric"].split('/'),
                      "type": 'counter',
                      "value": msg["value"]
                  }
                  collector.send(OutgoingMessageEnvelope(self.output, convert_to_statsd_format(metric)))
        except Exception, e:
            if (self.logger.isInfoEnabled):
                self.logger.info("Error while processing record" + str(data) + ": " + e.message)
            raise e

class StatsDTask(BaseTask):
    METRIC_GROUP_NAME = "statsd_push"
    
    def _init(self, config, context, metric_adaptor):
        statsd_host = config.get("rico.statsd.host")
        statsd_port = config.get("rico.statsd.port")
        enable_tcp = config.get("rico.tcp.enable", "false").lower() == "true"
        
        self.drop_secs = int(config.get("rico.drop.secs"))
        self.drop_old_msgs = True
        self.prefix = config.get("rico.statsd.prefix")
        
        self.logger.info("Drop secs: %s" % self.drop_secs)
        self.logger.info("Using TCP: %s" % enable_tcp)

        self.client = TCPStatsClient(statsd_host, statsd_port) if enable_tcp else StatsClient(statsd_host, statsd_port)
        
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
            if self.drop_old_msgs and time_diff_in_secs > self.drop_secs:
                self.logger.error("Time diff %ss is greater than configured maximum %ss...dropping msg %s" % (time_diff_in_secs, self.drop_secs, data))
                self.client.incr(self.get_own_stat_name(msg["source"], "old_messages_dropped"), 1)
            else:
                if self.logger.isDebugEnabled:
                    self.logger.debug("|".join([metric_type, name, str(msg['value'])]))
                if metric_type == "gauge":
                    self.client.gauge(name, msg["value"])
                elif metric_type == "counter":
                    self.client.incr(name, msg["value"])
                self.client.incr(self.get_own_stat_name(msg["source"], "messages_sent"), 1)
        except Exception, e:
            if (self.logger.isInfoEnabled):
                self.logger.info("Error while processing record" + str(data) + ": " + e.message)
            raise e
            
    def get_own_stat_name(self, source, name):
        return "%s.%s.%s.%s" % (self.prefix, self.METRIC_GROUP_NAME, source, name)
