import logging
import logging.config
import sys
import traceback


class LoggerBuilder(object):
    """Logger builder class which stores logger configuration"""

    def __init__(self):
        self.dictLogConfig = {
            'version': 1,
            'handlers': {
                'consoleHandler': {
                    'level': 'DEBUG',
                    'class': 'logging.StreamHandler',
                    'formatter': 'consoleFormatter',
                    'stream': 'ext://sys.stdout'
                }
            },
            'loggers': {
                '': {
                    'handlers': ['consoleHandler'],
                    'level': 'DEBUG',
                }
            },
            'formatters': {
                'consoleFormatter': {
                    'format': '%(asctime)s [%(levelname)s] %(message)s <%(name)s>'
                }
            }
        }

    def build(self):
        logging.config.dictConfig(self.dictLogConfig)
        return logging.getLogger()


def error_formatter(exc: Exception, neg_message: str = "", print_exc: bool = True) -> str:
    """Format error output.

    Args:
        exc - instance of exception
        neg_message - message witch will be presented before error stacktrace
        print_exc - if True print error to stderr

    Example stderr:
                    Negative message:
                       File "C:\scratch_63.py", line 320, in <module>
                         def_1()
                       File "C:\scratch_63.py", line 311, in def_1
                         return def_2()
                       File "C:\scratch_63.py", line 299, in def_5
                         raise ValueError("Now it is Value Error!")
                    Now it is Value Error!

    Return value:
        '\nNegative message:\n  File "C:\\scratch_63.py", line 320, in <module>\n    def_1()\n  File
        "C:\\scratch_63.py", line 311, in def_1\n    return def_5()\n  File "C:\\scratch_63.py", line 299,
        in def_5\n    raise ValueError("Now it is Value Error!")\nNow it is Value Error!'
    """
    if print_exc:
        print(f"\n{neg_message}", file=sys.stderr)
        traceback.print_tb(exc.__traceback__, file=sys.stderr)
        print(f"{exc}", file=sys.stderr)
    return f"\n{neg_message}\n{''.join(traceback.format_tb(exc.__traceback__))}{exc}"
