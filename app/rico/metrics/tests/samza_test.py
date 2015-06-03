import unittest
import mock
from rico.metrics.samza import *

class SamzaMetricsConverterTest(unittest.TestCase):

    def setUp(self):
        self.converter = SamzaMetricsConverter()

    def test_parse_kafka_highwater_mark(self):
        result = self.converter.parse_kafka_highwater_mark("kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-1-messages-behind-high-watermark")
        self.assertEquals({'stream': 'svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa', 'partition': '1'}, result)
        
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
            {'timestamp': 1433220776087, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.container.samza_container_0.kafka_consumer.stream.svc_s2_call_raw_wnqcfqaytreaowaa4ovsxa.partition.3.messages_behind_high_watermark', 'value': 987987},
            {'timestamp': 1433220776087, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.container.samza_container_0.kafka_consumer.stream.svc_s2_call_raw_wnqcfqaytreaowaa4ovsxa.partition.4.messages_behind_high_watermark', 'value': 8},
            {'timestamp': 1433220776087, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.container.samza_container_0.kafka_consumer.stream.svc_s2_call_raw_wnqcfqaytreaowaa4ovsxa.partition.5.messages_behind_high_watermark', 'value': 4}
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
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.task.TaskName_Partition_0.messages_sent', 'value': 5},
            {'timestamp': 1433220702299, 'type': 'gauge', 'name': 'samza.deploy_svc_repartition.1.task.TaskName_Partition_0.process_calls', 'value': 99}
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
            {'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.app_attempt_id', 'value': 'appattempt_1429549600644_0011_000001'},
            {'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.failed_containers', 'value': 0},
            {'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.job_healthy', 'value': 1},
            {'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.needed_containers', 'value': 0},
            {'timestamp': 1430179447515, 'type': 'gauge', 'name': 'samza.svc_call_join_deploy.1.app_master.running_containers', 'value': 1}
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
                    "processed": {
                        "oneMinuteRate": "4.4269901298545785E-9",
                        "meanRate": "130.25192997193733",
                        "count": "81974906",
                        "rateUnit": "SECONDS",
                        "type": "meter"
                    },
                    "lag-from-origin-ms": {
                        "75thPercentile": "3052.0",
                        "95thPercentile": "4051.0",
                        "mean": "2354.6792396139635",
                        "type": "histogram"
                    }
                }
            }
        }
        metrics = self.converter.get_statsd_metrics(data)
        expected = [
            {'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.lag-from-origin-ms.75thPercentile', 'value': '3052.0'},
            {'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.lag-from-origin-ms.95thPercentile', 'value': '4051.0'},
            {'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.lag-from-origin-ms.mean', 'value': '2354.6792396139635'},
            {'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.processed.count', 'value': '81974906'},
            {'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.processed.meanRate', 'value': '130.25192997193733'},
            {'timestamp': 1430179446591, 'type': 'gauge', 'name': 'samza.s2_call_parse.1.task.TaskName_Partition_6.rico.processed.oneMinuteRate', 'value': '4.4269901298545785E-9'}
        ]
        self.assertEquals(expected, sorted(metrics, key=lambda m: m['name']))
