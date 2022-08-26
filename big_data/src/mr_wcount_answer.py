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


class MRWordCountID2(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.tag_selector),
            MRStep(mapper=self.body_cleaner, reducer=self.word_counter),
            MRStep(mapper=self.dict_maker, reducer=self.top_filter),
        ]

    def tag_selector(self, _, line):
        """Categorize each line by post type and yield body of answers

        Input:
        - raw line from xml file
        Output:
        - key: None
        - value (answers): body in html format
        """
        # Use regex to get required xml tags
        regex_id = r'\sPostTypeId="(.*?)"'
        regex_body = r'\sBody="(.*?)"'
        mo_id = re.search(regex_id, line)
        mo_body = re.search(regex_body, line)

        post_id = mo_id.group(1) if mo_id else None
        post_body = mo_body.group(1) if mo_body else None

        if post_id == "2":
            logger.debug(f"post body:{post_body}")
            yield None, post_body

    def body_cleaner(self, _, body):
        """Remove html tags from body and split each word

        Output:
        - key: each word
        - value: '1' value for each occurence
        """
        if body:
            # Parse body with beautiful soup twice because of html format
            for i in range(2):
                # Add whitespaces if body lenght is shorter than 257 char to avoid bs4 warnings
                body_length = len(body)
                if body_length <= 256:
                    needed_char = 257 - body_length
                    body = body + (" " * needed_char)
                logger.debug(f"{body}")
                logger.debug("Beautiful Soup will run now")
                # Log possible errors to find bodies causing them
                try:
                    soup = BeautifulSoup(body, features="html.parser")
                    body = soup.get_text()
                except Exception as e:
                    logger.error(f"{body}")
                    logger.error(f"{e}")
                logger.debug("Beautiful Soup worked fine")

            # Normalize body text by lowering case and deleting special char
            body = body.lower()
            body = re.sub("[^a-z]+", " ", body)

            # Use list of stopwords to filter irrelevant words for the analysis
            stop_words = set(stopwords.words("english"))

            for word in body.split():
                if word not in stop_words:
                    logger.debug(f"{word}")
                    yield word, 1

    def word_counter(self, key, value):
        """Yield sum of occurences as value for each unique key"""
        yield key, sum(value)

    def dict_maker(self, key, value):
        """Yield None as key, and input in dictionary format as value"""
        yield None, {"word": key, "count": value}

    def top_filter(self, _, values):
        """Sort posts by view count

        output:
        - key: each of 10 most appearing words
        - value: number of occurences
        """
        # Sort words by occurences and keep required ones
        data = list(values)
        data.sort(key=lambda x: x.get("count"))
        data.reverse()
        data = data[:10]

        for item in data:
            yield item.values()


if __name__ == "__main__":
    start_time = time.time()
    MRWordCountID2.run()
    print()
    print(f"Execution time: {round((time.time() - start_time), 2)} seconds")
    print()
