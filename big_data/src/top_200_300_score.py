import logging
from datetime import datetime

from mrjob.job import MRJob
from mrjob.step import MRStep

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d",
)

lista_accepted = []
lista_answer = []


class top_10_most_viewed_posts(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_posts_dates,
            ),
            MRStep(
                combiner=self.combiner_posts_dates,
                reducer=self.reducer_top_200_300_score_answer_mean,
            ),
        ]

    def mapper_get_posts_dates(self, _, line):
        # yield posts id, score, creation date and accepted answer id
        # adds to lista_answer the id and the creation date

        if (
            line.find('PostTypeId="1"') != -1
            and line.find('AcceptedAnswerId="') != -1
        ):
            # Post Id
            id_split = line.split('<row Id="')
            id_split2 = id_split[1].split('" PostTypeId')
            id = int(id_split2[0])
            # Post creation date
            post_date = line.split('CreationDate="')
            post_date = post_date[1].split('" Score')
            post_date = post_date[0]
            # Post accepted answer id
            id_accepted_answer = line.split('AcceptedAnswerId="')
            id_accepted_answer = id_accepted_answer[1].split('" CreationDate')
            id_accepted_answer = int(id_accepted_answer[0])
            lista_accepted.append(id_accepted_answer)
            # Post id
            score_split = line.split('Score="')
            score_split2 = score_split[1].split('" ViewCount')
            score = int(score_split2[0])
            yield ("post_id:", id), {
                "score": score,
                "id_answer": id_accepted_answer,
                "post_date": post_date,
            }
        # if line its an answer, add to lista_answer answer id and answer
        # creation date
        if line.find('PostTypeId="2"') != -1:
            # Answer id
            id_split = line.split('<row Id="')
            id_split2 = id_split[1].split('" PostTypeId')
            id = int(id_split2[0])
            # Answer creation date
            answer_date = line.split('CreationDate="')
            answer_date = answer_date[1].split('" Score')
            answer_date = answer_date[0]
            answers = {"answer_id:": id, "date": answer_date}
            lista_answer.append(answers)

    def combiner_posts_dates(self, key, value):
        # Yields None and a dict with the post score and answer delay
        # Calculate answer delay
        for i in value:
            # Ignore answers id that are greater than the largest id of the file
            if i.get("id_answer") > 69093:
                continue

            post_date = i.get("post_date")
            answer_date = ""
            # look for an answer in list_answer that has the same id as in id_accepted_answer
            # to get the id creation date
            for answer in lista_answer:
                if answer.get("answer_id:") == i.get("id_answer"):
                    answer_date = answer.get("date")
            # change the date format
            post_date = datetime.strptime(post_date, "%Y-%m-%dT%H:%M:%S.%f")
            answer_date = datetime.strptime(
                answer_date, "%Y-%m-%dT%H:%M:%S.%f"
            )
            # calculate the difference in minutes
            answer_delay = int((answer_date - post_date).total_seconds()) / 60
            score = i.get("score")

            yield None, {"score": score, "value": answer_delay}

    def reducer_top_200_300_score_answer_mean(self, _, value):
        # the score and the answer delay of the posts in the top 200-300 by score

        lista = list(value)
        lista.sort(key=lambda x: x.get("score"))
        lista.reverse()
        lista = lista[199:299]
        promedio = 0
        # sum the answer delay
        for i in lista:
            promedio += i.get("value")
        promedio = promedio / 100
        yield "Lista", lista
        yield "Promedio:", promedio


if __name__ == "__main__":
    top_10_most_viewed_posts.run()
