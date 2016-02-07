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
import unittest
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

    def test_container_elasticsearch_http_producer_metrics(self):
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
                "com.quantiply.samza.system.elasticsearch.ElasticsearchSystemProducerMetrics": {
                  "es-bulk-send-wait-ms": {
                    "75thPercentile": 164,
                    "98thPercentile": 164,
                    "min": 8,
                    "median": 8,
                    "95thPercentile": 164,
                    "99thPercentile": 164,
                    "max": 164,
                    "mean": 85.41501096850321,
                    "999thPercentile": 164,
                    "type": "histogram",
                    "stdDev": 77.99780630141484
                  },
                  "es-bulk-send-trigger-max-actions": 1,
                  "es-lag-from-receive-ms": {
                    "75thPercentile": 675,
                    "98thPercentile": 677,
                    "min": 167,
                    "median": 173,
                    "95thPercentile": 677,
                    "99thPercentile": 677,
                    "max": 677,
                    "mean": 306.81746184974213,
                    "999thPercentile": 677,
                    "type": "histogram",
                    "stdDev": 220.75271582756383
                  },
                  "es-bulk-send-trigger-flush-cmd": 0,
                  "es-inserts": 7,
                  "es-updates": 12,
                  "es-bulk-send-batch-size": {
                    "75thPercentile": 20,
                    "98thPercentile": 20,
                    "min": 7,
                    "median": 7,
                    "95thPercentile": 20,
                    "99thPercentile": 20,
                    "max": 20,
                    "mean": 13.451250914041934,
                    "999thPercentile": 20,
                    "type": "histogram",
                    "stdDev": 6.49981719178457
                  },
                  "es-bulk-send-trigger-max-interval": 1,
                  "es-lag-from-origin-ms": {
                    "75thPercentile": 0,
                    "98thPercentile": 0,
                    "min": 0,
                    "median": 0,
                    "95thPercentile": 0,
                    "99thPercentile": 0,
                    "max": 0,
                    "mean": 0,
                    "999thPercentile": 0,
                    "type": "histogram",
                    "stdDev": 0
                  },
                  "es-bulk-send-success": 2,
                  "es-version-conflicts": 5,
                  "es-deletes": 3
                }
            }
        }
        metrics = self.converter.get_statsd_metrics(data)
        print sorted(metrics, key=lambda m: m['name'])
        expected = [
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_batch_size.75thPercentile', 'value': 20, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_batch_size.95thPercentile', 'value': 20, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_batch_size.98thPercentile', 'value': 20, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_batch_size.999thPercentile', 'value': 20, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_batch_size.99thPercentile', 'value': 20, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_batch_size.max', 'value': 20, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_batch_size.mean', 'value': 13.451250914041934, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_batch_size.median', 'value': 7, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_batch_size.min', 'value': 7, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_batch_size.stdDev', 'value': 6.49981719178457, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_success', 'value': 2, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_trigger_flush_cmd', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_trigger_max_actions', 'value': 1, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_trigger_max_interval', 'value': 1, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_wait_ms.75thPercentile', 'value': 164, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_wait_ms.95thPercentile', 'value': 164, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_wait_ms.98thPercentile', 'value': 164, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_wait_ms.999thPercentile', 'value': 164, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_wait_ms.99thPercentile', 'value': 164, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_wait_ms.max', 'value': 164, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_wait_ms.mean', 'value': 85.41501096850321, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_wait_ms.median', 'value': 8, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_wait_ms.min', 'value': 8, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_bulk_send_wait_ms.stdDev', 'value': 77.99780630141484, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_deletes', 'value': 3, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_inserts', 'value': 7, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_origin_ms.75thPercentile', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_origin_ms.95thPercentile', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_origin_ms.98thPercentile', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_origin_ms.999thPercentile', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_origin_ms.99thPercentile', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_origin_ms.max', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_origin_ms.mean', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_origin_ms.median', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_origin_ms.min', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_origin_ms.stdDev', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_receive_ms.75thPercentile', 'value': 675, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_receive_ms.95thPercentile', 'value': 677, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_receive_ms.98thPercentile', 'value': 677, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_receive_ms.999thPercentile', 'value': 677, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_receive_ms.99thPercentile', 'value': 677, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_receive_ms.max', 'value': 677, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_receive_ms.mean', 'value': 306.81746184974213, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_receive_ms.median', 'value': 173, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_receive_ms.min', 'value': 167, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_lag_from_receive_ms.stdDev', 'value': 220.75271582756383, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_updates', 'value': 12, 'source': 'samza'},
            {'timestamp': 1433547788717, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.container.samza_container_0.eshttp.producer.es_version_conflicts', 'value': 5, 'source': 'samza'},
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

    def test_kv_metrics(self):
        data = {
            "header": {
                "job-id": "1",
                "samza-version": "0.9.0",
                "job-name": "foo",
                "host": "thedude",
                "reset-time": 1433220701927,
                "container-name": "samza-container-0",
                "source": "TaskName-Partition 0",
                "time": 1433220702299,
                "version": "0.0.1"
            },
            "metrics": {
                "org.apache.samza.storage.kv.KeyValueStoreMetrics": {
                    "localstore-flushes": 417,
                    "localstore-alls": 0,
                    "localstore-deletes": 0,
                    "localstore-puts": 11158,
                    "localstore-bytes-read": 9466370,
                    "localstore-ranges": 0,
                    "localstore-gets": 3932,
                    "localstore-bytes-written": 27026294
                },
                "org.apache.samza.storage.kv.CachedStoreMetrics": {
                    "localstore-flushes": 417,
                    "localstore-cache-size": 1000,
                    "localstore-alls": 0,
                    "localstore-deletes": 0,
                    "localstore-flush-batch-size": 11158,
                    "localstore-puts": 84482,
                    "localstore-ranges": 0,
                    "localstore-gets": 78239,
                    "localstore-dirty-count": 0,
                    "localstore-cache-hits": 74307
                },
                "org.apache.samza.storage.kv.KeyValueStorageEngineMetrics": {
                    "localstore-flushes": 413,
                    "localstore-alls": 0,
                    "localstore-deletes": 0,
                    "localstore-puts": 84482,
                    "localstore-messages-restored": 0,
                    "localstore-ranges": 0,
                    "localstore-gets": 78239,
                    "localstore-messages-bytes": 0
                },
                "org.apache.samza.storage.kv.SerializedKeyValueStoreMetrics": {
                    "localstore-flushes": 417,
                    "localstore-alls": 0,
                    "localstore-bytes-deserialized": 9466370,
                    "localstore-deletes": 0,
                    "localstore-puts": 11158,
                    "localstore-ranges": 0,
                    "localstore-gets": 3932,
                    "localstore-bytes-serialized": 27082664
                }
            }
        }

        metrics = self.converter.get_statsd_metrics(data)
        print(sorted(metrics, key=lambda m: m['name']))
        expected = [
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.cached_store.alls', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.cached_store.deletes', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.cached_store.flushes', 'value': 417, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.cached_store.gets', 'value': 78239, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.cached_store.puts', 'value': 84482, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.cached_store.ranges', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.engine.alls', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.engine.deletes', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.engine.flushes', 'value': 413, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.engine.gets', 'value': 78239, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.engine.puts', 'value': 84482, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.engine.ranges', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.serialized_store.alls', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.serialized_store.deletes', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.serialized_store.flushes', 'value': 417, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.serialized_store.gets', 'value': 3932, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.serialized_store.puts', 'value': 11158, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.serialized_store.ranges', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.store.alls', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.store.deletes', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.store.flushes', 'value': 417, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.store.gets', 'value': 3932, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.store.puts', 'value': 11158, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore.store.ranges', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore_bytes.serialized_store.deserialized', 'value': 9466370, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore_bytes.serialized_store.serialized', 'value': 27082664, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore_bytes.store.read', 'value': 9466370, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore_bytes.store.written', 'value': 27026294, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore_cache.cached_store.hits', 'value': 74307, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore_cache.cached_store.size', 'value': 1000, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore_dirty.cached_store.count', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore_flush_batch.cached_store.size', 'value': 11158, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore_messages.engine.bytes', 'value': 0, 'source': 'samza'},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.foo.1.task.TaskName_Partition_0.kv.localstore_messages.engine.restored', 'value': 0, 'source': 'samza'}
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
