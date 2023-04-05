#from colorlog import ColoredFormatter
import logging
import os

if not os.path.exists(os.path.join("src", "logs")):
    os.mkdir(os.path.join("src", "logs"))

LOGFORMAT = "%(asctime)s : %(levelname)s | %(message)s"

logging.basicConfig(
    filename=os.path.join("src", "logs", "last_run.log"),
    filemode="w",
    level = logging.DEBUG,
    format = LOGFORMAT
)

stream = logging.StreamHandler()
stream.setLevel(logging.DEBUG)
stream.setFormatter(logging.Formatter(LOGFORMAT))

logging.getLogger("").addHandler(stream)

LOG_MAIN = logging.getLogger("Main")
LOG_GET = logging.getLogger("GetData")
LOG_DB = logging.getLogger("Database")