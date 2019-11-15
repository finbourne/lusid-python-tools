import logging
import coloredlogs


class LusidLogger:
    def __init__(self, log_level="info"):
        self.begin_logger(log_level)
        pass

    @staticmethod
    def begin_logger(log_level) -> None:
        """
        This function gets an instance of the root logger and sets the log_level. 
        :param log_level: A string defining what log level to set logger at
        :return:
        """
        set_logger_level = {
            "notset": logging.NOTSET,
            "info": logging.INFO,
            "debug": logging.DEBUG,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }

        if not log_level:
            log_level = "info"

        if log_level not in set_logger_level.keys():
            raise Exception(
                f"logging level provided ({log_level}) is not in list of valid logging levels {list(set_logger_level.keys())}"
            )

        # set up logging
        root_logger = logging.getLogger()
        root_logger.setLevel(set_logger_level[log_level])
        coloredlogs.install(level=set_logger_level[log_level], logger=root_logger)
