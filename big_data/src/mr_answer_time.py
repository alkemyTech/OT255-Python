import logging
import logging.config
import os
import re
import sys
import time
from pathlib import Path

import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep

os.chdir(sys.path[0])

logging.config.fileConfig(
    os.path.join(Path.cwd(), "../log/logger_h.cfg"),
    defaults={"filename": "mr_answer_time.log"},
)

logger = logging.getLogger("logger_h")
logger.disabled = True


class MRLessViewed(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.post_filter, reducer=self.dict_concatenator),
            MRStep(mapper=self.score_filter, reducer=self.list_concatenator),
            MRStep(mapper=self.df_joiner),
        ]

    def post_filter(self, _, line):
        regex_type = r'\sPostTypeId="(.*?)"'
        mo_type = re.search(regex_type, line)

        post_type = mo_type.group(1) if mo_type else None

        if post_type == "1":
            regex_id = r'row Id="(.*?)"'
            regex_answer = r'\sAcceptedAnswerId="(.*?)"'
            regex_date = r'\sCreationDate="(.*?)"'
            regex_score = r'\sScore="(.*?)"'
            mo_id = re.search(regex_id, line)
            mo_answer = re.search(regex_answer, line)
            mo_date = re.search(regex_date, line)
            mo_score = re.search(regex_score, line)

            question_id = mo_id.group(1) if mo_id else None
            question_answer = mo_answer.group(1) if mo_answer else None
            question_date = mo_date.group(1) if mo_date else None
            question_score = mo_score.group(1) if mo_score else None

            if question_answer is not None:
                logger.debug(
                    f"| question_id: {question_id} | question_score: {question_score} | question_date: {question_date} | question_answer: {question_answer} |"
                )

                yield post_type, {
                    "question_id": question_id,
                    "question_score": int(question_score),
                    "question_date": question_date,
                    "question_answer": question_answer,
                }

        if post_type == "2":
            regex_id = r'row Id="(.*?)"'
            regex_date = r'\sCreationDate="(.*?)"'
            mo_id = re.search(regex_id, line)
            mo_date = re.search(regex_date, line)

            answer_id = mo_id.group(1) if mo_id else None
            answer_date = mo_date.group(1) if mo_date else None
            logger.debug(f"| answer_id: {answer_id} | answer_date: {answer_date} |")

            yield post_type, {"answer_id": answer_id, "answer_date": answer_date}

    def dict_concatenator(self, key, values):
        yield key, list(values)

    def score_filter(self, key, values):
        if key == "1":
            values.sort(key=lambda x: x.get("question_score"), reverse=True)
            values = values[300:400]
            for i, value in enumerate(values):
                logger.debug(f"{i}: {value}")
            yield None, {"key": key, "values": values}

        if key == "2":
            yield None, {"key": key, "values": values}

    def list_concatenator(self, _, values):
        yield None, list(values)

    def df_joiner(self, _, values):
        dict_questions = (
            values[0]["values"] if values[0]["key"] == "1" else values[1]["values"]
        )
        df_questions = pd.DataFrame(dict_questions)

        dict_answers = (
            values[0]["values"] if values[0]["key"] == "2" else values[1]["values"]
        )
        df_answers = pd.DataFrame(dict_answers)

        df_total = pd.merge(
            df_questions,
            df_answers,
            how="left",
            left_on="question_answer",
            right_on="answer_id",
        )

        df_total["question_date"] = pd.to_datetime(df_total["question_date"])
        df_total["answer_date"] = pd.to_datetime(df_total["answer_date"])

        df_total["date_diff"] = df_total["answer_date"] - df_total["question_date"]

        # json_df = df_total.to_json(date_format="iso", orient="records")
        # logger.debug(f"{json_df}")

        diff_mean = df_total["date_diff"].mean(numeric_only=False)
        logger.debug(f"{diff_mean}")

        yield None, str(diff_mean)

    # def dummy_reducer(self, _, result):
    #     yield None, result


if __name__ == "__main__":
    start_time = time.time()
    MRLessViewed.run()
    print()
    print(f"Execution time: {round((time.time() - start_time), 2)} seconds")
    print()
