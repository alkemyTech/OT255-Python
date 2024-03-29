import logging
import logging.config
import os
import sys

# Change cwd directory to current folder to keep .py/.cfg/.log together.
os.chdir(sys.path[0])

logging.config.fileConfig(
    os.path.join(sys.path[0], "logger_h.cfg"),
    defaults={"filename": "current_filename.log"},
)

logger = logging.getLogger("logger_h")

logger.debug("Trying logger configuration")
