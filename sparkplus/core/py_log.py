import logging
import logging.handlers
import datetime

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "[%(asctime)s[%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s"
)

stremaHandler = logging.StreamHandler()
now = str(datetime.datetime.now()).split(".")[0]
fileHandler = logging.FileHandler("../logs/" + now)
logger.setLevel(level=logging.DEBUG)

stremaHandler.setFormatter(formatter)
fileHandler.setFormatter(formatter)

logger.addHandler(stremaHandler)
logger.addHandler(fileHandler)


logger.setLevel(level=logging.DEBUG)
logging.debug("DEBUG log")
logging.info("INFO log")
logging.warning("WARN log")
logging.error("ERROR log")
logging.critical("CRITICAL log")
