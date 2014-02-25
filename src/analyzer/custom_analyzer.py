import logging
from Queue import Empty
from redis import StrictRedis
from time import time, sleep
from threading import Thread
from collections import defaultdict
from multiprocessing import Process, Manager, Queue
from msgpack import Unpacker, unpackb, packb
from os import path, kill, getpid, system
from math import ceil
import traceback
import operator
import socket
import settings

from alerters import trigger_alert
from algorithms import run_selected_custom_algorithm
from algorithm_exceptions import *
from analyzer import Analyzer
import algorithms

logger = logging.getLogger("CustomAnalyzerLog")


class CustomAnalyzer(Analyzer):
    def __init__(self, parent_pid, name, metric_filter, algorithms, consensus, alerter):
        """
        Initialize the Analyzer
        """
        super(CustomAnalyzer, self).__init__(parent_pid)
        self.name = name
        self.metric_filter = metric_filter
        self.algorithms = algorithms

        # Ensure algorithms with custom conditions are curried
        for alg in self.algorithms:
            if type(alg) is tuple:
                # alg will be of the format (name, kwargs)
                algorithms.curry_algorithm_to_global(alg[0], alg[1])

        self.consensus = consensus
        self.alerter = alerter

    def spin_process(self, i, custom_metrics):
        """
        Assign a bunch of custom metrics for a process to analyze.
        """
        # Discover assigned metrics
        num_processes = min(len(custom_metrics), settings.CUSTOM_ANALYZER_PROCESSES)
        keys_per_processor = int(ceil(float(len(custom_metrics)) / float(num_processes)))
        if i == num_processes:
            assigned_max = len(custom_metrics)
        else:
            assigned_max = i * keys_per_processor
        assigned_min = assigned_max - keys_per_processor
        assigned_keys = range(assigned_min, assigned_max)

        # Compile assigned metrics
        assigned_metrics = [custom_metrics[index] for index in assigned_keys]

        # Check if this process is unnecessary
        if len(assigned_metrics) == 0:
            return

        # Multi get series
        raw_assigned = self.redis_conn.mget(assigned_metrics)

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        # Distill timeseries strings into lists
        for i, metric_name in enumerate(assigned_metrics):
            self.check_if_parent_is_alive()

            try:
                raw_series = raw_assigned[i]
                unpacker = Unpacker(use_list = False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)

                anomalous, ensemble, datapoint = run_selected_custom_algorithm(
                    timeseries, metric_name, self.algorithms, self.consensus,
                )

                # If it's anomalous, add it to list
                if anomalous:
                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    metric = [datapoint, base_name]
                    self.anomalous_metrics.append(metric)

                    # Get the anomaly breakdown - who returned True?
                    for index, value in enumerate(ensemble):
                        if value:
                            algorithm = self.algorithms[index]
                            anomaly_breakdown[algorithm] += 1

            # It could have been deleted by the Roomba
            except TypeError:
                exceptions['DeletedByRoomba'] += 1
            except TooShort:
                exceptions['TooShort'] += 1
            except Stale:
                exceptions['Stale'] += 1
            except Boring:
                exceptions['Boring'] += 1
            except:
                exceptions['Other'] += 1
                logger.info(traceback.format_exc())

        # Add values to the queue so the parent process can collate
        for key, value in anomaly_breakdown.items():
            self.anomaly_breakdown_q.put((key, value))

        for key, value in exceptions.items():
            self.exceptions_q.put((key, value))

    def run(self):
        """
        Called when the process intializes.
        """
        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.error('skyline can\'t connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                self.redis_conn = StrictRedis(unix_socket_path = settings.REDIS_SOCKET_PATH)
                continue

            # Discover unique metrics
            unique_metrics = list(self.redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))

            # Filter for custom metrics
            custom_metrics = [custom for custom in unique_metrics 
                                if self.metric_filter in custom]

            if len(custom_metrics) == 0:
                logger.info('no metrics in redis for this filter')
                sleep(10)
                continue

            # Spawn processes
            pids = []
            num_processes = min(len(custom_metrics), settings.CUSTOM_ANALYZER_PROCESSES)
            for i in range(1, num_processes):
                p = Process(target=self.spin_process, args=(i, custom_metrics))
                pids.append(p)
                p.start()

            # Send wait signal to zombie processes
            for p in pids:
                p.join()

            # Grab data from the queue and populate dictionaries
            exceptions = dict()
            anomaly_breakdown = dict()
            while 1:
                try:
                    key, value = self.anomaly_breakdown_q.get_nowait()
                    if key not in anomaly_breakdown.keys():
                        anomaly_breakdown[key] = value
                    else:
                        anomaly_breakdown[key] += value
                except Empty:
                    break

            while 1:
                try:
                    key, value = self.exceptions_q.get_nowait()
                    if key not in exceptions.keys():
                        exceptions[key] = value
                    else:
                        exceptions[key] += value
                except Empty:
                    break

            # Send alerts
            if settings.ENABLE_ALERTS:
                for alert in settings.ALERTS:
                    if alert[0] == self.name:
                        for metric in self.anomalous_metrics:
                            cache_key = 'last_alert.%s.%s' % (self.name, metric[1])
                            try:
                                last_alert = self.redis_conn.get(cache_key)
                                if not last_alert:
                                    self.redis_conn.setex(cache_key, alert[2], packb(metric[0]))
                                    trigger_alert(alert, metric)

                            except Exception as e:
                                logger.error("couldn't send alert: %s" % e)

            # Skip writing to web

            # Log progress
            logger.info('Custom Analyser   :: %s' % (self.name))
            logger.info('seconds to run    :: %.2f' % (time() - now))
            logger.info('total metrics     :: %d' % len(custom_metrics))
            logger.info('total analyzed    :: %d' % (len(custom_metrics) - sum(exceptions.values())))
            logger.info('total anomalies   :: %d' % len(self.anomalous_metrics))
            logger.info('exception stats   :: %s' % exceptions)
            logger.info('anomaly breakdown :: %s' % anomaly_breakdown)

            # Log to Graphite
            self.send_graphite_metric('skyline.custom_analyzer.%s.run_time' % self.name, '%.2f' % (time() - now))
            self.send_graphite_metric('skyline.custom_analyzer.%s.total_analyzed' % self.name, '%.2f' % (len(custom_metrics) - sum(exceptions.values())))

            # Reset counters
            self.anomalous_metrics[:] = []

            # Sleep if it went too fast
            if time() - now < 5:
                logger.info('sleeping due to low run time...')
                sleep(10)
