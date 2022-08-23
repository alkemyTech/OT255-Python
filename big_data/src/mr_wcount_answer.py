import logging
import logging.config
import os
import re
import sys
import time
from pathlib import Path

# import nltk
from bs4 import BeautifulSoup
from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.corpus import stopwords

os.chdir(sys.path[0])

logging.config.fileConfig(
    os.path.join(Path.cwd(), "../log/logger_h.cfg"),
    defaults={"filename": "mr_wcount_answer.log"},
)

logger = logging.getLogger("logger_h")
logger.disabled = True


class MRWordCountID1(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.post_filter),
            MRStep(mapper=self.body_cleaner, reducer=self.word_counter),
            MRStep(mapper=self.dict_maker, reducer=self.top_filter),
        ]

    def post_filter(self, _, line):
        regex_id = r'\sPostTypeId="(.*?)"'
        regex_body = r'\sBody="(.*?)"'
        mo_id = re.search(regex_id, line)
        mo_body = re.search(regex_body, line)

        post_id = mo_id.group(1) if mo_id else None
        post_body = mo_body.group(1) if mo_body else None

        # logger.debug(f"post id:{post_id}")

        if post_id == "2":
            # logger.debug(f"post body:{post_body}")
            yield None, post_body

    def body_cleaner(self, _, body):

        if body:
            for i in range(2):
                body_length = len(body)
                if body_length <= 256:
                    needed_char = 257 - body_length
                    body = body + (" " * needed_char)
                logger.debug(f"{body}")
                logger.debug("Beautiful Soup will run now")
                try:
                    soup = BeautifulSoup(body, features="html.parser")
                    body = soup.get_text()
                except Exception as e:
                    logger.error(f"{body}")
                    logger.error(f"{e}")
                logger.debug("Beautiful Soup worked fine")

            body = body.lower()

            body = re.sub("[^a-z]+", " ", body)

            # nltk.download("stopwords")
            stop_words = set(stopwords.words("english"))

            for word in body.split():
                if word not in stop_words:
                    logger.debug(f"{word}")
                    yield word, 1

    def word_counter(self, key, value):
        yield key, sum(value)

    def dict_maker(self, key, value):
        logger.debug(f"key is: {key} - value is: {value}")
        yield None, {"word": key, "count": value}

    def top_filter(self, _, values):
        data = list(values)
        data.sort(key=lambda x: x.get("count"))
        data.reverse()
        data = data[:10]

        for item in data:
            yield item.values()


if __name__ == "__main__":
    start_time = time.time()
    MRWordCountID1.run()
    print()
    print(f"Execution time: {round((time.time() - start_time), 2)} seconds")
    print()
