import unittest
import mock
from rico.metrics import *
from org.apache.samza.system import SystemStream, OutgoingMessageEnvelope

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