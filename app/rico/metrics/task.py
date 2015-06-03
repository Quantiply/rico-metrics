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
    name = ".".join([replace_non_alphanum(data[i]) for i in name_cols])
    return { "timestamp": data["timestamp"], "name": name, "value" : data["value"], "type" : data["type"]}

class SamzaMetricsTask(BaseTask):
    KAFKA_SYSTEM_CONSUMER_METRIC_GRP_NAME = "org.apache.samza.system.kafka.KafkaSystemConsumerMetrics"
  
    def _init(self, config, context, metric_adaptor):
        self.output = self.getSystemStream("out")
        self.registerDefaultHandler(self.handle_msg)

    def handle_msg(self, data, collector, coord):
        try:
            self.logger.debug(str(data))
            source = data.message["header"]["source"]
            if data.message.has_key("asMap"): #For Samza 0.8.0 compatibility
                body = data.message["asMap"]
            else:
                body = data.message
            if source.startswith("ApplicationMaster"):
                self.am_metrics(body, collector)
            elif source.startswith("TaskName"):
                self.task_metrics(body, collector)
            elif source == data.message["header"]["container-name"]:
                self.container_metrics(body, collector)
        except Exception, e:
            self.logger.error(traceback.format_exc())
            if self.logger.isDebugEnabled:
                self.logger.debug("Message was: " + str(data))
            raise e

    def container_metrics(self, data, collector):
        if self.KAFKA_SYSTEM_CONSUMER_METRIC_GRP_NAME in data["metrics"]:
            self.kafka_consumer_metrics(data, collector)

    def parse_kafka_highwater_mark(self, metric):
        #kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-1-messages-behind-high-watermark
        result = re.match('^kafka-(.+)-(\d+)-messages-behind-high-watermark$', metric)
        if not result:
            raise Exception("Failed to parse Kafka highwater mark metric")
        return {'topic': result.group(1), 'partition': result.group(2)}

    def kafka_consumer_metrics(self, data, collector):
        metrics = data["metrics"][self.KAFKA_SYSTEM_CONSUMER_METRIC_GRP_NAME]
        for (metric, val) in metrics.iteritems():
            if metric.endswith('messages-behind-high-watermark'):
                parsed = self.parse_kafka_highwater_mark(metric)
                metric = {
                    "name": name,
                    "type": 'gauge',
                    "value": 
                }
                self.send_metric(data, metric, collector)

    def task_metrics(self, data, collector):
        task_metrics = "org.apache.samza.container.TaskInstanceMetrics"

        # Process calls
        self.send_simple_metric("process-calls", task_metrics, data, "counter", collector)

        # Messages sent
        self.send_simple_metric("messages-sent", task_metrics, data, "counter", collector)
        
        self.rico_metrics(data, collector)
        
    def rico_metrics(self, data, collector):
        RICO_GROUP_NAME = "com.quantiply.rico"
        if data["metrics"].has_key(RICO_GROUP_NAME):
            rico_metrics = data["metrics"][RICO_GROUP_NAME]
            for (metric_name, metric_vals) in rico_metrics.iteritems():
                self.rico_metric(data, metric_name, metric_vals, collector)
                    
    def rico_metric(self, data, metric_name, metric_vals, collector):
        for (metric_attr, metric_val) in metric_vals.iteritems():
            if metric_attr == "type" or metric_attr == "rateUnit":
                continue
            header = data["header"]
            m = {}
            m["timestamp"] = header["time"]
            m["source"] = "samza"
            m["job_id"] = replace_non_alphanum(header["job-id"])
            m["job_name"] = replace_non_alphanum(header["job-name"])
            m["task_id"] = replace_non_alphanum(header["source"])
            m["metric_attr"] =  metric_attr
            m["metric_name"] = metric_name #(Don't scrub - dots in names are meaningful)
            m["type"] = "gauge"
            m["value"] = replace_non_alphanum(metric_val)

            # Format : <prefix>.<source>.<job_name>.<job_id>.<task_id>.<metric_name>.<metric_attr>
            name_cols = ["source", "job_name", "job_id", "task_id", "metric_name", "metric_attr"]
            name = ".".join([m[i] for i in name_cols])
            payload = { "timestamp": m["timestamp"], "name": name, "value" : m["value"], "type" : m["type"]}
            collector.send(OutgoingMessageEnvelope(self.output, payload))

    def am_metrics(self, data, collector):
        am = "org.apache.samza.job.yarn.SamzaAppMasterMetrics"
        # healthy jobs
        self.send_simple_metric("job-healthy", am, data, "gauge", collector)

        # running containers
        self.send_simple_metric("running-containers", am, data, "gauge", collector)

        # failed containers
        self.send_simple_metric("failed-containers", am, data, "gauge", collector)
        

    def add_common_fields(self, data, metric):
        header = data["header"]

        metric["timestamp"] = header["time"]
        metric["source"] = "samza"
        metric["job_id"] = header["job-id"]
        metric["job_name"] = header["job-name"]
        metric["metric_source"] = header["source"]

    def send_simple_metric(self, name, group, data, metric_type , collector):
        metric = {
            "name": name,
            "type": metric_type,
            "value": data["metrics"][group][name]
        }
        self.send_metric(data, metric, collector)
    
    def send_metric(self, data, metric, collector, names=None):
        """
        Format metric for statsd and send to output topic

        Args:
          data (dict): the original Samza metrics structure
          metric (dict): a dictionary for the metric with name, type, and value fields
          collector (org.apache.samza.task.MessageCollector): the Samza message collector object
          names (list of str, optional): override default list of names
        """
        DEFAULT_NAMES = ['source', 'job_name', 'job_id', 'metric_source', 'name']
        names = names or DEFAULT_NAMES
        self.add_common_fields(data, metric)
        collector.send(OutgoingMessageEnvelope(self.output, convert_to_statsd_format(names, metric)))
        
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

