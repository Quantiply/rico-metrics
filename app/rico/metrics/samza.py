import re
from rico.metrics.statsd import convert_to_statsd_format

class SamzaMetricsConverter(object):
    KAFKA_SYSTEM_CONSUMER_GRP_NAME = "org.apache.samza.system.kafka.KafkaSystemConsumerMetrics"
    TASK_GRP_NAME = 'org.apache.samza.container.TaskInstanceMetrics'
    YARN_APP_MASTER_GRP_NAME = 'org.apache.samza.job.yarn.SamzaAppMasterMetrics'

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
          samza_metrics (dict): python dict from Samza metrics JSON

        Returns:
          List of statsd messages, each with keys: timestamp, name, value, type

        """
        stats = []
        
        stats += self.get_app_master_metrics(samza_metrics)
        stats += self.get_task_metrics(samza_metrics)
        stats += self.get_kafka_consumer_metrics(samza_metrics)
        
        return stats

    #samza.s2_call_parse.1.TaskName_Partition_2.lag_from_origin_ms_min

    #samza.<job-name>.<job-id>.container.<container-name>.kafka.consumer.stream.<stream>.partition.<pid>.msgs-behind-broker
    
    #samza.s2_call_parse.1.container.<>
    
    def get_task_metrics(self, samza_metrics):
        statsd_metrics = []
        if self.TASK_GRP_NAME in samza_metrics['metrics']:
            #samza.<job-name>.<job-id>.task.<TaskName_Partition_2>.<metric>
            metrics = samza_metrics['metrics'][self.TASK_GRP_NAME]
            hdr = samza_metrics['header']
            for name in ['process-calls', 'messages-sent']:
                metric = {
                    "timestamp": hdr['time'],
                    "name_list": ['samza', hdr['job-name'], hdr['job-id'], 'task', hdr['source'], name],
                    "type": 'gauge',
                    "value": metrics[name]
                }
                statsd_metrics.append(convert_to_statsd_format(metric))
        return statsd_metrics

    def get_app_master_metrics(self, samza_metrics):
        statsd_metrics = []
        if self.YARN_APP_MASTER_GRP_NAME in samza_metrics['metrics']:
            #samza.<job-name>.<job-id>.app-master.<metric>
            metrics = samza_metrics['metrics'][self.YARN_APP_MASTER_GRP_NAME]
            hdr = samza_metrics['header']
            for name in ['job-healthy', 'needed-containers', 'running-containers', 'failed-containers', 'app-attempt-id']:
                metric = {
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

