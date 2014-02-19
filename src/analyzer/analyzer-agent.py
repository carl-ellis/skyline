import logging
import sys
import traceback
from os import getpid
from os.path import dirname, abspath, isdir
from daemon import runner
from time import sleep, time

# add the shared settings file to namespace
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import settings

from analyzer import Analyzer
from custom_analyzer import CustomAnalyzer


class AnalyzerAgent():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = settings.LOG_PATH + '/analyzer.log'
        self.stderr_path = settings.LOG_PATH + '/analyzer.log'
        self.pidfile_path = settings.PID_PATH + '/analyzer.pid'
        self.pidfile_timeout = 5

    def run(self):
        logger.info('starting skyline analyzer')
        Analyzer(getpid()).start()

        self._run_custom()

        while 1:
            sleep(100)


    def _run_custom(self):
        """ Starts custom analysers based off configuration in settings """
        custom_analyser_settings = settings.CUSTOM_ANALYSERS
        for custom_name, custom_settings in custom_analyser_settings.items():
            CustomAnalyzer(getpid(), custom_name, **custom_settings).start()


if __name__ == "__main__":
    """
    Start the Analyzer agent.
    """
    if not isdir(settings.PID_PATH):
        print 'pid directory does not exist at %s' % settings.PID_PATH
        sys.exit(1)

    if not isdir(settings.LOG_PATH):
        print 'log directory does not exist at %s' % settings.LOG_PATH
        sys.exit(1)

    # Make sure we can run all the algorithms
    try:
        from algorithms import *
        timeseries = map(list, zip(map(float, range(int(time()) - 86400, int(time()) + 1)), [1] * 86401))
        ensemble = [globals()[algorithm](timeseries) for algorithm in settings.ALGORITHMS]
    except KeyError as e:
        print "Algorithm %s deprecated or not defined; check settings.ALGORITHMS" % e
        sys.exit(1)
    except Exception as e:
        print "Algorithm test run failed."
        traceback.print_exc()
        sys.exit(1)

    analyzer = AnalyzerAgent()

    logger = logging.getLogger("AnalyzerLog")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.FileHandler(settings.LOG_PATH + '/analyzer.log')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    clogger = logging.getLogger("CustomAnalyzerLog")
    clogger.setLevel(logging.DEBUG)
    cformatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    chandler = logging.FileHandler(settings.LOG_PATH + '/custom_analyzers.log')
    chandler.setFormatter(cformatter)
    clogger.addHandler(chandler)

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        analyzer.run()
    else:
        daemon_runner = runner.DaemonRunner(analyzer)
        daemon_runner.daemon_context.files_preserve = [handler.stream, chandler.stream]
        daemon_runner.do_action()
