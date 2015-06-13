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
from datetime import datetime
from rico.metrics.task import SamzaMetricsTask, DruidMetricsTask, StatsDTask
from org.apache.samza.system import SystemStream, OutgoingMessageEnvelope

class SamzaMetricsTaskTest(unittest.TestCase):
    
    def setUp(self):
        self.task = SamzaMetricsTask()
        self.task.output = SystemStream("fakeSys", "fakeStream")
        
    def call_handle_msg(self, data):
        self.mock_collector = mock.Mock()
        self.mock_coordinator = mock.Mock()
        self.mock_envelope = mock.Mock()
        self.mock_envelope.message = data
        self.task.handle_msg(self.mock_envelope, self.mock_collector, self.mock_coordinator)

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
                    "kafka-svc.s2.call.raw.wnqcfqaytreaowaa4ovsxa-4-messages-behind-high-watermark": 0,
                }
            }
        }
        
        self.call_handle_msg(data)
        
        expected = {'type': 'gauge', 'name': 'samza.s2_call_parse.1.container.samza_container_0.kafka_consumer.stream.svc_s2_call_raw_wnqcfqaytreaowaa4ovsxa.partition.4.messages_behind_high_watermark', 'value': 0, 'timestamp': 1433220776087L}
        self.mock_collector.send.assert_called_once_with(OutgoingMessageEnvelope(self.task.output, expected))


class DruidMetricsTaskTest(unittest.TestCase):
    
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
        
        expected = {'type': 'counter', 'name': 'druid.middlemanager.fb_agg_mm_0_dev_quantezza_com_8089.datasource.wikipedia.events.processed', 'value': 0, 'timestamp': datetime(2015, 4, 22, 19, 31, 20, 896000)}
        self.mock_collector.send.assert_called_once_with(OutgoingMessageEnvelope(self.task.output, expected))

    def test_persist(self):
        self.call_handle_msg({"feed": "metrics", "user2": "wikipedia", "service": "middlemanager", "timestamp": "2015-04-22T19:31:20.896Z", "metric": "persists/num", "value": 0, "host": "fb-agg-mm-0.dev.quantezza.com:8089"})
        
        expected = {'type': 'counter', 'name': 'druid.middlemanager.fb_agg_mm_0_dev_quantezza_com_8089.datasource.wikipedia.persists.num', 'value': 0, 'timestamp': datetime(2015, 4, 22, 19, 31, 20, 896000)}
        self.mock_collector.send.assert_called_once_with(OutgoingMessageEnvelope(self.task.output, expected))

    def test_exec(self):
        self.call_handle_msg({"feed": "metrics", "service": "druid/sit/realtime", "timestamp": "2015-06-02T18:02:24.713Z", "metric": "exec/backlog", "value": 1, "host": "thedude:8101"})
        
        expected = {'type': 'counter', 'name': 'druid.druid_sit_realtime.thedude_8101.node.exec.backlog', 'value': 1, 'timestamp': datetime(2015, 6, 2, 18, 2, 24, 713000)}
        self.mock_collector.send.assert_called_once_with(OutgoingMessageEnvelope(self.task.output, expected))

    def test_cache(self):
        self.call_handle_msg({"feed": "metrics", "service": "druid/sit/broker", "timestamp": "2015-06-02T18:02:10.372Z", "metric": "cache/delta/hitRate", "value": 0.0, "host": "thedude:8080"})
        
        expected = {'type': 'counter', 'name': 'druid.druid_sit_broker.thedude_8080.node.cache.delta.hitRate', 'value': 0.0, 'timestamp': datetime(2015, 6, 2, 18, 2, 10, 372000)}
        self.mock_collector.send.assert_called_once_with(OutgoingMessageEnvelope(self.task.output, expected))

    def test_query(self):
        self.call_handle_msg({"feed": "metrics", "user6": "false", "user4": "timeseries", "user5": ["2015-06-02T17:00:00.000Z/2015-06-02T18:02:13.800Z"], "user2": "svc_perf", "service": "druid/sit/realtime", "user7": "2 aggs", "timestamp": "2015-06-02T18:02:12.660Z", "metric": "query/time", "value": 4, "user9": "PT62M", "host": "thedude:8101", "user8": "d240be3d-59d0-4c6d-8310-189a281bbdd7"})
        
        expected = {'type': 'counter', 'name': 'druid.druid_sit_realtime.thedude_8101.datasource.svc_perf.query.time', 'value': 4, 'timestamp': datetime(2015, 6, 2, 18, 2, 12, 660000)}
        self.mock_collector.send.assert_called_once_with(OutgoingMessageEnvelope(self.task.output, expected))

    def test_jvm(self):
        self.call_handle_msg({"feed": "metrics", "user2": "PS Perm Gen", "service": "middlemanager", "user1": "nonheap", "timestamp": "2015-04-22T19:31:17.878Z", "metric": "jvm/pool/committed", "value": 83886080, "host": "fb-agg-mm-0.dev.quantezza.com:8189"})
        
        expected = {'type': 'counter', 'name': 'druid.middlemanager.fb_agg_mm_0_dev_quantezza_com_8189.node.jvm.pool.committed', 'value': 83886080, 'timestamp': datetime(2015, 4, 22, 19, 31, 17, 878000)}
        self.mock_collector.send.assert_called_once_with(OutgoingMessageEnvelope(self.task.output, expected))

class StatsDTaskTest(unittest.TestCase):
    
    def setUp(self):
        self.task = StatsDTask()
        self.task.client = mock.Mock()
        self.task.drop_old_msgs = False
        self.task.prefix = "fake.prefix"

    def test_msg(self):
        envelope = mock.Mock()
        envelope.message = {"type":"gauge","name":"samza.s2_call_parse.1.TaskName_Partition_6.streams.default.dropped.meanRate","value":0.2,"timestamp":1433220836231}
        self.task.handle_msg(envelope, None, None)
        self.task.client.gauge.assert_called_once_with("fake.prefix.samza.s2_call_parse.1.TaskName_Partition_6.streams.default.dropped.meanRate", 0.2)

if __name__ == '__main__':
    unittest.main()