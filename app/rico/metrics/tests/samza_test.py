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
import unittest
import mock
from rico.metrics.samza import SamzaMetricsConverter

class SamzaMetricsConverterTest(unittest.TestCase):

    def setUp(self):
        self.converter = SamzaMetricsConverter()

    def test_parse_kafka_highwater_mark(self):
        result = self.converter.parse_kafka_highwater_mark("kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-1-messages-behind-high-watermark")
        self.assertEquals({'stream': 'svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa', 'partition': '1'}, result)

    def test_container_elasticsearch_producer_metrics(self):
        data = {
            "header": {
                "job-id": "1",
                "samza-version": "0.9.0",
                "job-name": "deploy-svc-repartition",
                "host": "thedude",
                "reset-time": 1433533612817,
                "container-name": "samza-container-0",
                "source": "samza-container-0",
                "time": 1433547788717,
                "version": "0.0.1"
            },
            "metrics": {
                "org.apache.samza.system.elasticsearch.ElasticsearchSystemProducerMetrics": {
                    "es-docs-updated": 14,
                    "es-bulk-send-success": 4,
                    "es-docs-inserted": 0,
                    "es-version-conflicts": 7
                }
            }
        }
        metrics = self.converter.get_statsd_metrics(data)
        expected = [
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.es.producer.es_bulk_send_success', 'value': 4, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.es.producer.es_docs_inserted', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.es.producer.es_docs_updated', 'value': 14, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.es.producer.es_version_conflicts', 'value': 7, 'source': 'samza'}
        ]
        self.assertEquals(expected, sorted(metrics, key=lambda m: m['name']))

    def test_container_jvm_metrics(self):
        data = {
            "header": {
                "job-id": "1",
                "samza-version": "0.9.0",
                "job-name": "deploy-svc-repartition",
                "host": "thedude",
                "reset-time": 1433533612817,
                "container-name": "samza-container-0",
                "source": "samza-container-0",
                "time": 1433547788717,
                "version": "0.0.1"
            },
            "metrics": {
                "org.apache.samza.metrics.JvmMetrics": {
                    "mem-heap-committed-mb": 276.5,
                    "ps scavenge-gc-time-millis": 3073,
                    "mem-non-heap-used-mb": 38.16989
                }
            }
        }
        metrics = self.converter.get_statsd_metrics(data)
        expected = [
            {'source': 'samza', 'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.jvm.mem_heap_committed_mb', 'value': 276.5},
            {'source': 'samza', 'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.jvm.mem_non_heap_used_mb', 'value': 38.16989},
            {'source': 'samza', 'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.jvm.ps_scavenge_gc_time_millis', 'value': 3073}
        ]
        self.assertEquals(expected, sorted(metrics, key=lambda m: m['name']))
        
    def test_kafka_consumer_lag(self):
        data = {
            "header": {
                "job-id": "1",
                "samza-version": "0.9.0",
                "job-name": "s2-call-parse",
                "host": "thedude",
                "reset-time": 1433220715640,
                "container-name": "samza-container-0",
                "source": "samza-container-0",
                "time": 1433220776087,
                "version": "0.0.1"
            },
            "metrics": {
                "org.apache.samza.system.kafka.KafkaSystemConsumerMetrics": {
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-4-messages-behind-high-watermark": 8,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-3-messages-behind-high-watermark": 987987,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-5-messages-behind-high-watermark": 4,
                }
            }
        }
        
        metrics = self.converter.get_statsd_metrics(data)
        expected = [
            {'source': 'samza', 'timestamp': 1433220776087, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.container.samza_container_0.kafka_consumer.stream.svc_s2_call_raw_wnqcfqaytreaowaa4ovsxa.partition.3.messages_behind_high_watermark', 'value': 987987},
            {'source': 'samza', 'timestamp': 1433220776087, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.container.samza_container_0.kafka_consumer.stream.svc_s2_call_raw_wnqcfqaytreaowaa4ovsxa.partition.4.messages_behind_high_watermark', 'value': 8},
            {'source': 'samza', 'timestamp': 1433220776087, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.container.samza_container_0.kafka_consumer.stream.svc_s2_call_raw_wnqcfqaytreaowaa4ovsxa.partition.5.messages_behind_high_watermark', 'value': 4}
        ]
        self.assertEquals(expected, sorted(metrics, key=lambda m: m['name']))

    def test_task_metrics(self):
        data = {
            "header": {
                "job-id": "1",
                "samza-version": "0.9.0",
                "job-name": "deploy-svc-repartition",
                "host": "thedude",
                "reset-time": 1433220701927,
                "container-name": "samza-container-0",
                "source": "TaskName-Partition 0",
                "time": 1433220702299,
                "version": "0.0.1"
            },
            "metrics": {
                "org.apache.samza.container.TaskInstanceMetrics": {
                    "commit-calls": 1,
                    "window-calls": 0,
                    "flush-calls": 1,
                    "send-calls": 0,
                    "process-calls": 99,
                    "messages-sent": 5,
                    "kafka-deploy.svc.fwqahei5r7wypqaasf3mkg-0-offset": None
                }
            }
        }
        
        metrics = self.converter.get_statsd_metrics(data)
        expected = [
            {'source': 'samza', 'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.task.TaskName_Partition_0.messages_sent', 'value': 5},
            {'source': 'samza', 'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.task.TaskName_Partition_0.process_calls', 'value': 99}
        ]
        self.assertEquals(expected, sorted(metrics, key=lambda m: m['name']))

    def test_app_master_metrics(self):
        data = {
            "header": {
                "job-id": "1",
                "samza-version": "0.9.0",
                "job-name": "svc-call-join-deploy",
                "host": "sit321w80m7",
                "reset-time": 1429911471329,
                "container-name": "ApplicationMaster",
                "source": "ApplicationMaster",
                "time": 1430179447515,
                "version": "0.0.1"
            },
            "metrics": {
                "org.apache.samza.metrics.JvmMetrics": {
                    "mem-heap-committed-mb": 308,
                    "threads-waiting": 5,
                    "threads-timed-waiting": 30,
                    "threads-runnable": 11,
                    "mem-heap-used-mb": 103.311905,
                    "ps scavenge-gc-count": 16,
                    "ps scavenge-gc-time-millis": 243,
                    "gc-count": 21,
                    "mem-non-heap-used-mb": 49.18598,
                    "threads-new": 0,
                    "ps marksweep-gc-count": 5,
                    "threads-blocked": 0,
                    "gc-time-millis": 655,
                    "mem-non-heap-committed-mb": 50.148438,
                    "ps marksweep-gc-time-millis": 412,
                    "threads-terminated": 0
                },
                "org.apache.samza.job.yarn.SamzaAppMasterMetrics": {
                    "job-healthy": 1,
                    "http-port": 59670,
                    "completed-containers": 0,
                    "needed-containers": 0,
                    "running-containers": 1,
                    "failed-containers": 0,
                    "rpc-port": 38361,
                    "released-containers": 0,
                    "app-attempt-id": "appattempt_1429549600644_0011_000001",
                    "task-count": 1,
                    "http-host": "thedude"
                }
            }
        }
        metrics = self.converter.get_statsd_metrics(data)
        expected = [
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.app_attempt_id', 'value': 'appattempt_1429549600644_0011_000001'},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.failed_containers', 'value': 0},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.job_healthy', 'value': 1},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.gc_count', 'value': 21},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.gc_time_millis', 'value': 655},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.mem_heap_committed_mb', 'value': 308},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.mem_heap_used_mb', 'value': 103.311905},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.mem_non_heap_committed_mb', 'value': 50.148438},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.mem_non_heap_used_mb', 'value': 49.18598},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.ps_marksweep_gc_count', 'value': 5},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.ps_marksweep_gc_time_millis', 'value': 412},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.ps_scavenge_gc_count', 'value': 16},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.ps_scavenge_gc_time_millis', 'value': 243},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.threads_blocked', 'value': 0},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.threads_new', 'value': 0},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.threads_runnable', 'value': 11},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.threads_terminated', 'value': 0},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.threads_timed_waiting', 'value': 30},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.jvm.threads_waiting', 'value': 5},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.needed_containers', 'value': 0},
            {'source': 'samza', 'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.running_containers', 'value': 1}
        ]
        self.assertEquals(expected, sorted(metrics, key=lambda m: m['name']))

    def test_rico_metrics(self):
        data = {
            "header": {
                "job-id": "1",
                "samza-version": "0.9.0",
                "job-name": "s2-call-parse",
                "host": "thedude",
                "reset-time": 1429550083069,
                "container-name": "samza-container-0",
                "source": "TaskName-Partition 6",
                "time": 1430179446591,
                "version": "0.0.1"
            },
            "metrics": {
                "com.quantiply.rico": {
                    "streams.default.processed": {
                        "oneMinuteRate": 4.4269,
                        "meanRate": 130.25192997193733,
                        "count": 81974906,
                        "rateUnit": "SECONDS",
                        "type": "meter"
                    },
                    "streams.default.lag-from-origin-ms": {
                        "75thPercentile": 3052.0,
                        "95thPercentile": 4051.0,
                        "mean": 2354.6792396139635,
                        "type": "histogram"
                    },
                    "streams.default.max-lag-by-origin-ms": {
                      "data": {
                        "sit229w80m7-sit:ets:s2:ord-stderr": 6690721007
                      },
                      "type": "windowed-map",
                      "window-duration-ms": 60000
                    },
                }
            }
        }
        metrics = self.converter.get_statsd_metrics(data)
        expected = [
            {'source': 'samza', 'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.streams.default.lag-from-origin-ms.75thPercentile', 'value': 3052.0},
            {'source': 'samza', 'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.streams.default.lag-from-origin-ms.95thPercentile', 'value': 4051.0},
            {'source': 'samza', 'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.streams.default.lag-from-origin-ms.mean', 'value': 2354.6792396139635},
            {'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.streams.default.max-lag-by-origin-ms.sit229w80m7_sit_ets_s2_ord_stderr', 'value': 6690721007, 'source': 'samza'},
            {'source': 'samza', 'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.streams.default.processed.count', 'value': 81974906},
            {'source': 'samza', 'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.streams.default.processed.meanRate', 'value': 130.25192997193733},
            {'source': 'samza', 'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.streams.default.processed.oneMinuteRate', 'value': 4.4269}
        ]
        self.assertEquals(expected, sorted(metrics, key=lambda m: m['name']))
