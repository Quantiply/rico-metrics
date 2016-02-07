#
# Copyright 2014-2016 Quantiply Corporation. All rights reserved.
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
from rico.metrics.statsd_util import convert_to_statsd_format, format_name

class SamzaMetricsConverter(object):
    KAFKA_SYSTEM_CONSUMER_GRP_NAME = "org.apache.samza.system.kafka.KafkaSystemConsumerMetrics"
    TASK_GRP_NAME = 'org.apache.samza.container.TaskInstanceMetrics'
    YARN_APP_MASTER_GRP_NAME = 'org.apache.samza.job.yarn.SamzaAppMasterMetrics'
    JVM_GRP_NAME = 'org.apache.samza.metrics.JvmMetrics'
    ES_NATIVE_PRODUCER_GRP_NAME = 'org.apache.samza.system.elasticsearch.ElasticsearchSystemProducerMetrics'
    ES_HTTP_PRODUCER_GRP_NAME = 'com.quantiply.samza.system.elasticsearch.ElasticsearchSystemProducerMetrics'
    KV_GRP_NAMES = {
      'org.apache.samza.storage.kv.KeyValueStoreMetrics': 'store',
      'org.apache.samza.storage.kv.CachedStoreMetrics': 'cached-store',
      'org.apache.samza.storage.kv.KeyValueStorageEngineMetrics': 'engine',
      'org.apache.samza.storage.kv.SerializedKeyValueStoreMetrics': 'serialized-store'
    }
    RICO_GRP_NAME = 'com.quantiply.rico'

    def parse_kafka_highwater_mark(self, metric):
        #kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-1-messages-behind-high-watermark
        result = re.match('^kafka-(.+)-(\d+)-messages-behind-high-watermark$', metric)
        if not result:
            raise Exception("Failed to parse Kafka highwater mark metric")
        return {'stream': result.group(1), 'partition': result.group(2)}


    def get_statsd_metrics(self, samza_metrics):
        """
        Format data for statsd topic

        Args:
          samza_metrics (dict): python dict from Samza metrics JSON msg

        Returns:
          List of statsd messages, each with keys: timestamp, name, value, type

        """
        hdr = samza_metrics['header']

        if hdr['source'] == 'ApplicationMaster':
            return self.get_app_master_metrics(samza_metrics)

        if hdr['source'] == hdr['container-name']:
            return self.get_container_metrics(samza_metrics)

        return self.get_task_metrics(samza_metrics)

    def get_container_metrics(self, samza_metrics):
        stats = []
        stats += self.get_container_jvm_metrics(samza_metrics)
        stats += self.get_kafka_consumer_metrics(samza_metrics)
        stats += self.get_container_elasticsearch_metrics(samza_metrics)
        return stats

    def get_container_jvm_metrics(self, samza_metrics):
        statsd_metrics = []
        if self.JVM_GRP_NAME in samza_metrics['metrics']:
            #samza.<job-name>.<job-id>.container.<container-name>.jvm.<metric>
            metrics = samza_metrics['metrics'][self.JVM_GRP_NAME]
            hdr = samza_metrics['header']
            for name in metrics.keys():
                metric = {
                    "source": "samza",
                    "timestamp": hdr['time'],
                    "name_list": [
                        'samza', hdr['job-name'], hdr['job-id'], 'container', hdr['container-name'],
                        'jvm', name
                    ],
                    "type": 'gauge',
                    "value": metrics[name]
                }
                statsd_metrics.append(convert_to_statsd_format(metric))
        return statsd_metrics

    def get_container_elasticsearch_metrics(self, samza_metrics):
        stats = []
        stats += self.get_container_elasticsearch_native_metrics(samza_metrics)
        stats += self.get_container_elasticsearch_http_metrics(samza_metrics)
        return stats

    def get_container_elasticsearch_http_metrics(self, samza_metrics):
        statsd_metrics = []
        if self.ES_HTTP_PRODUCER_GRP_NAME in samza_metrics['metrics']:
            metrics = samza_metrics['metrics'][self.ES_HTTP_PRODUCER_GRP_NAME]
            hdr = samza_metrics['header']
            partial_name_list = ['samza', hdr['job-name'], hdr['job-id'], 'container', hdr['container-name'], 'eshttp', 'producer']
            for name in metrics.keys():
                if isinstance(metrics[name], dict):
                    #samza.<job-name>.<job-id>.container.<container-name>.eshttp.producer.<metric-name>.<metric_attr>
                    statsd_metrics += self.get_rico_coda_metrics(format_name(name), metrics[name], hdr, partial_name_list)
                else:
                    #samza.<job-name>.<job-id>.container.<container-name>.eshttp.producer.<metric>
                    metric = {
                        "source": "samza",
                        "timestamp": hdr['time'],
                        "name_list": partial_name_list + [name],
                        "type": 'gauge',
                        "value": metrics[name]
                    }
                    statsd_metrics.append(convert_to_statsd_format(metric))
        return statsd_metrics

    def get_container_elasticsearch_native_metrics(self, samza_metrics):
        statsd_metrics = []
        if self.ES_NATIVE_PRODUCER_GRP_NAME in samza_metrics['metrics']:
            #samza.<job-name>.<job-id>.container.<container-name>.es.producer.<metric>
            metrics = samza_metrics['metrics'][self.ES_NATIVE_PRODUCER_GRP_NAME]
            hdr = samza_metrics['header']
            for name in metrics.keys():
                metric = {
                    "source": "samza",
                    "timestamp": hdr['time'],
                    "name_list": [
                        'samza', hdr['job-name'], hdr['job-id'], 'container', hdr['container-name'],
                        'es', 'producer', name
                    ],
                    "type": 'gauge',
                    "value": metrics[name]
                }
                statsd_metrics.append(convert_to_statsd_format(metric))
        return statsd_metrics

    def get_rico_metrics(self, samza_metrics):
        statsd_metrics = []
        if self.RICO_GRP_NAME in samza_metrics['metrics']:
            #samza.<job-name>.<job-id>.task.<task-name>.rico.<metric-name>.<metric-attr>
            #   NOTE: <metric-name> not escaped - the dots have meaning
            metrics = samza_metrics['metrics'][self.RICO_GRP_NAME]
            hdr = samza_metrics['header']
            for (metric_name, metric_attrs) in metrics.iteritems():
                if metric_attrs["type"] == "windowed-map":
                    statsd_metrics += self.get_rico_windowed_map_metrics(metric_name, metric_attrs, hdr)
                else:
                    statsd_metrics += self.get_rico_coda_task_metrics(metric_name, metric_attrs, hdr)
        return statsd_metrics

    def get_rico_windowed_map_metrics(self, metric_name, metric_attrs, hdr):
        statsd_metrics = []
        #samza.<job-name>.<job-id>.task.<task-name>.rico.<metric-name>.<key>

        #Iterate through "data"
        for (key, val) in metric_attrs["data"].iteritems():
            name_list = [format_name(n) for n in ('samza', hdr['job-name'], hdr['job-id'], 'task', hdr['source'])] \
                + ['rico', metric_name, format_name(key)]
            metric = {
                "source": "samza",
                "timestamp": hdr['time'],
                "name_list": name_list,
                "type": 'gauge',
                "value": val
            }
            statsd_metrics.append(convert_to_statsd_format(metric, format_names=False))
        return statsd_metrics

    def get_rico_coda_task_metrics(self, metric_name, metric_attrs, hdr):
        #samza.<job-name>.<job-id>.task.<task-name>.rico.<metric-name>.<metric-attr>
        partial_name_list = ['samza', hdr['job-name'], hdr['job-id'], 'task', hdr['source'], 'rico']
        return self.get_rico_coda_metrics(metric_name, metric_attrs, hdr, partial_name_list)

    def get_rico_coda_metrics(self, metric_name, metric_attrs, hdr, partial_name_list):
        """
        partial_name_list is the list of names for the metric except the metric_name and metric_attr
        NOTE: metric_name is not escaped - dots will have meaning
        """
        statsd_metrics = []
        for (metric_attr, val) in metric_attrs.iteritems():
            if metric_attr == "type" or metric_attr == "rateUnit":
                continue
            metric = {
                "source": "samza",
                "timestamp": hdr['time'],
                "name_list": [format_name(n) for n in partial_name_list] + [metric_name, metric_attr],
                "type": 'gauge',
                "value": val
            }
            statsd_metrics.append(convert_to_statsd_format(metric, format_names=False))
        return statsd_metrics

    def get_task_metrics(self, samza_metrics):
        stats = []
        stats += self.get_task_builtin_metrics(samza_metrics)
        stats += self.get_task_kv_store_metrics(samza_metrics)
        stats += self.get_rico_metrics(samza_metrics)
        return stats

    def get_task_builtin_metrics(self, samza_metrics):
        statsd_metrics = []
        if self.TASK_GRP_NAME in samza_metrics['metrics']:
            #samza.<job-name>.<job-id>.task.<task-name>.<metric>
            metrics = samza_metrics['metrics'][self.TASK_GRP_NAME]
            hdr = samza_metrics['header']
            for name in ['process-calls', 'messages-sent']:
                metric = {
                    "source": "samza",
                    "timestamp": hdr['time'],
                    "name_list": ['samza', hdr['job-name'], hdr['job-id'], 'task', hdr['source'], name],
                    "type": 'gauge',
                    "value": metrics[name]
                }
                statsd_metrics.append(convert_to_statsd_format(metric))
        return statsd_metrics

    def get_task_kv_store_metrics(self, samza_metrics):
        statsd_metrics = []

        for (grp_name, kv_grp_metric) in self.KV_GRP_NAMES.iteritems():
          if grp_name in samza_metrics['metrics']:
            #samza.<job-name>.<job-id>.task.<task-name>.kv.<store>.<kv_grp_metric>.<metric>
            metrics = samza_metrics['metrics'][grp_name]
            hdr = samza_metrics['header']
            for name in metrics.keys():
              #Split store_name from metric
              (store_name, metric) = name.rsplit('-', 1)
              metric = {
                  "source": "samza",
                  "timestamp": hdr['time'],
                  "name_list": ['samza', hdr['job-name'], hdr['job-id'], 'task', hdr['source'], 'kv', store_name, kv_grp_metric, metric],
                  "type": 'gauge',
                  "value": metrics[name]
              }
              statsd_metrics.append(convert_to_statsd_format(metric))
        return statsd_metrics

    def get_app_master_metrics(self, samza_metrics):
        return self.get_app_master_jvm_metrics(samza_metrics) \
            + self.get_app_master_health_metrics(samza_metrics)

    def get_app_master_jvm_metrics(self, samza_metrics):
        statsd_metrics = []
        if self.JVM_GRP_NAME in samza_metrics['metrics']:
            #samza.<job-name>.<job-id>.app-master.jvm.<metric>
            metrics = samza_metrics['metrics'][self.JVM_GRP_NAME]
            hdr = samza_metrics['header']
            for name in metrics.keys():
                metric = {
                    "source": "samza",
                    "timestamp": hdr['time'],
                    "name_list": ['samza', hdr['job-name'], hdr['job-id'], 'app-master', 'jvm', name],
                    "type": 'gauge',
                    "value": metrics[name]
                }
                statsd_metrics.append(convert_to_statsd_format(metric))
        return statsd_metrics

    def get_app_master_health_metrics(self, samza_metrics):
        statsd_metrics = []
        if self.YARN_APP_MASTER_GRP_NAME in samza_metrics['metrics']:
            #samza.<job-name>.<job-id>.app-master.<metric>
            metrics = samza_metrics['metrics'][self.YARN_APP_MASTER_GRP_NAME]
            hdr = samza_metrics['header']
            for name in ['job-healthy', 'needed-containers', 'running-containers', 'failed-containers', 'app-attempt-id']:
                metric = {
                    "source": "samza",
                    "timestamp": hdr['time'],
                    "name_list": ['samza', hdr['job-name'], hdr['job-id'], 'app-master', name],
                    "type": 'gauge',
                    "value": metrics[name]
                }
                statsd_metrics.append(convert_to_statsd_format(metric))
        return statsd_metrics

    def get_kafka_consumer_metrics(self, samza_metrics):
        statsd_metrics = []
        if self.KAFKA_SYSTEM_CONSUMER_GRP_NAME in samza_metrics["metrics"]:
            for (metric, val) in samza_metrics["metrics"][self.KAFKA_SYSTEM_CONSUMER_GRP_NAME].iteritems():
                #samza.<job-name>.<job-id>.container.<container-name>.kafka.consumer.stream.<stream>.partition.<pid>.messages-behind-high-watermark
                if metric.endswith('messages-behind-high-watermark'):
                    parsed = self.parse_kafka_highwater_mark(metric)
                    hdr = samza_metrics['header']
                    metric = {
                        "source": "samza",
                        "timestamp": hdr['time'],
                        "name_list": [
                            'samza', hdr['job-name'], hdr['job-id'], 'container', hdr['container-name'],
                            'kafka-consumer', 'stream', parsed['stream'], 'partition', parsed['partition'],
                            'messages-behind-high-watermark'
                        ],
                        "type": 'gauge',
                        "value": val
                    }
                    statsd_metrics.append(convert_to_statsd_format(metric))
        return statsd_metrics
