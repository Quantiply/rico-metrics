import unittest
import mock
from rico.metrics import *
from org.apache.samza.system import SystemStream, OutgoingMessageEnvelope

class SamzaToStatsD(unittest.TestCase):
    
    def setUp(self):
        self.task = SamzaMetricsTask()
        self.task.output = SystemStream("fakeSys", "fakeStream")
        
    def call_handle_msg(self, data):
        self.mock_collector = mock.Mock()
        self.mock_coordinator = mock.Mock()
        self.mock_envelope = mock.Mock()
        self.mock_envelope.message = data
        self.task.handle_msg(self.mock_envelope, self.mock_collector, self.mock_coordinator)
        
    def test_parse_kafka_highwater_mark(self):
        task = SamzaMetricsTask()
        result = task.parse_kafka_highwater_mark("kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-1-messages-behind-high-watermark")
        self.assertEquals({'topic': 'svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa', 'partition': '1'}, result)
        
    def test_system_consumer_metrics(self):
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
                    "no-more-messages-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 6]": True,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-0-messages-read": 2,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-2-bytes-read": 776,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-4-high-watermark": 2,
                    "no-more-messages-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 2]": True,
                    "kafka-thedude-9092-reconnects": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-4-messages-behind-high-watermark": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-0-offset-change": 2,
                    "blocking-poll-timeout-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 1]": 0,
                    "blocking-poll-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 5]": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-1-messages-behind-high-watermark": 0,
                    "buffered-message-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 3]": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-1-offset-change": 1,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-4-messages-read": 2,
                    "blocking-poll-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 1]": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-5-bytes-read": 776,
                    "blocking-poll-timeout-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 5]": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-5-offset-change": 2,
                    "buffered-message-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 7]": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-6-messages-read": 2,
                    "no-more-messages-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 3]": True,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-2-messages-behind-high-watermark": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-6-high-watermark": 2,
                    "no-more-messages-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 7]": True,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-1-messages-read": 1,
                    "blocking-poll-timeout-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 0]": 0,
                    "blocking-poll-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 4]": 0,
                    "buffered-message-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 2]": 0,
                    "blocking-poll-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 0]": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-7-high-watermark": 2,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-2-offset-change": 2,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-6-offset-change": 2,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-0-bytes-read": 773,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-5-messages-read": 2,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-7-bytes-read": 777,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-3-messages-behind-high-watermark": 0,
                    "blocking-poll-timeout-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 4]": 0,
                    "buffered-message-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 6]": 0,
                    "no-more-messages-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 4]": True,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-0-messages-behind-high-watermark": 0,
                    "no-more-messages-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 0]": True,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-7-messages-read": 2,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-2-messages-read": 2,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-6-bytes-read": 835,
                    "buffered-message-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 1]": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-0-high-watermark": 2,
                    "blocking-poll-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 3]": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-1-bytes-read": 387,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-7-offset-change": 2,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-5-high-watermark": 2,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-3-offset-change": 2,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-5-messages-behind-high-watermark": 0,
                    "blocking-poll-timeout-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 7]": 4900,
                    "buffered-message-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 5]": 0,
                    "blocking-poll-timeout-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 3]": 1,
                    "blocking-poll-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 7]": 0,
                    "poll-count": 4903,
                    "kafka-thedude-9092-topic-partitions": 8,
                    "no-more-messages-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 5]": True,
                    "kafka-thedude-9092-messages-read": 581,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-4-bytes-read": 776,
                    "no-more-messages-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 1]": True,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-6-messages-behind-high-watermark": 0,
                    "kafka-thedude-9092-skipped-fetch-requests": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-1-high-watermark": 1,
                    "buffered-message-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 0]": 0,
                    "blocking-poll-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 2]": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-3-high-watermark": 2,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-3-messages-read": 2,
                    "kafka-thedude-9092-bytes-read": 5893,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-3-bytes-read": 793,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-2-high-watermark": 2,
                    "blocking-poll-timeout-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 6]": 1,
                    "buffered-message-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 4]": 0,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-4-offset-change": 2,
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-7-messages-behind-high-watermark": 0,
                    "blocking-poll-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 6]": 0,
                    "blocking-poll-timeout-count-SystemStreamPartition [kafka, svc.s2.call.raw.wNqcfQaYTReaowAA4OVSxA, 2]": 1
                }
            }
        }
        
        self.call_handle_msg(data)
        
        expected = {'type': 'counter', 'name': 'druid.middlemanager.fb_agg_mm_0_dev_quantezza_com_8089.wikipedia.events_processed', 'value': 0, 'timestamp': datetime.datetime(2015, 4, 22, 19, 31, 20, 896000)}
        self.mock_collector.send.assert_called_once_with(OutgoingMessageEnvelope(self.task.output, expected))


class DruidToStatsD(unittest.TestCase):
    
    def setUp(self):
        self.task = DruidMetricsTask()
        self.task.output = SystemStream("fakeSys", "fakeStream")
        
    def call_handle_msg(self, data):
        self.mock_collector = mock.Mock()
        self.mock_coordinator = mock.Mock()
        self.mock_envelope = mock.Mock()
        self.mock_envelope.message = data
        self.task.handle_msg(self.mock_envelope, self.mock_collector, self.mock_coordinator)
        
    def test_events(self):
        self.call_handle_msg({"feed": "metrics", "user2": "wikipedia", "service": "middlemanager", "timestamp": "2015-04-22T19:31:20.896Z", "metric": "events/processed", "value": 0, "host": "fb-agg-mm-0.dev.quantezza.com:8089"})
        
        expected = {'type': 'counter', 'name': 'druid.middlemanager.fb_agg_mm_0_dev_quantezza_com_8089.wikipedia.events_processed', 'value': 0, 'timestamp': datetime.datetime(2015, 4, 22, 19, 31, 20, 896000)}
        self.mock_collector.send.assert_called_once_with(OutgoingMessageEnvelope(self.task.output, expected))

    def test_persist(self):
        self.call_handle_msg({"feed": "metrics", "user2": "wikipedia", "service": "middlemanager", "timestamp": "2015-04-22T19:31:20.896Z", "metric": "persists/num", "value": 0, "host": "fb-agg-mm-0.dev.quantezza.com:8089"})
        
        expected = {'type': 'counter', 'name': 'druid.middlemanager.fb_agg_mm_0_dev_quantezza_com_8089.wikipedia.persists_num', 'value': 0, 'timestamp': datetime.datetime(2015, 4, 22, 19, 31, 20, 896000)}
        self.mock_collector.send.assert_called_once_with(OutgoingMessageEnvelope(self.task.output, expected))

    def test_filtered(self):
        self.call_handle_msg({"feed": "metrics", "user2": "PS Perm Gen", "service": "middlemanager", "user1": "nonheap", "timestamp": "2015-04-22T19:31:17.878Z", "metric": "jvm/pool/committed", "value": 83886080, "host": "fb-agg-mm-0.dev.quantezza.com:8189"})
        self.assertFalse(self.mock_collector.send.called)

if __name__ == '__main__':
    unittest.main()