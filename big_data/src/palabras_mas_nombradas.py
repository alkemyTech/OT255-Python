import nltk
from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.corpus import stopwords

stop_words = stopwords.words("english")


class top_10_words_per_tag(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_tags,
                combiner=self.combiner_tags,
                reducer=self.reducer_find_top_10_words_per_tag,
            ),
            MRStep(
                # combiner=self.combiner_tags,
                reducer=self.reducer_find_top_10_words_per_tag_f,
            ),
            MRStep(
                # combiner=self.combiner_tags,
                reducer=self.reducer_find_top_10_words_per_tag_f1,
            ),
        ]

    def mapper_get_tags(self, _, line):
        # yield the tag and each word in the body
        if line.find('Tags="&lt;') != -1:

            tag_split = line.split('Tags="&lt;')
            tag_split2 = tag_split[1].split("&gt;")
            tag = tag_split2[0]

            lista_strings = [
                "&lt",
                "p&gt",
                "&#xA",
                "a&gt",
                "Q&amp",
                ";amp;",
                "&quot;ah",
                ";em&gt",
                "li&gt",
                "strong&gt",
                "blockquote",
                ";",
                "/",
                "?",
                ":",
                "(",
                ")",
            ]
            palabras = line.split('Body="&lt;')
            palabras_cuerpo = palabras[1].split('" OwnerUserId=')

            body = palabras_cuerpo[0]
            for i in lista_strings:
                body = body.replace(i, "")
            body = body.replace(".", " ")
            body = body.lower()
            words = body.split(" ")
            for word in words:
                if word in stop_words:
                    continue
                elif word != "":
                    yield (tag, word), 1

    def combiner_tags(self, key, value):

        yield None, {"key": key, "value": sum(value)}

    def reducer_find_top_10_words_per_tag(self, key, value):
        value = list(value)
        # value.sort(key=lambda x: x.get("key")[0])
        value.sort(reverse=True, key=lambda x: x.get("value"))

        for i in value:
            yield (i.values())

    def reducer_find_top_10_words_per_tag_f(self, key, value):
        yield None, (key, sum(value))

    def reducer_find_top_10_words_per_tag_f1(self, key, value):
        value = list(value)
        value.sort(key=lambda x: (x[0][0], -x[1]))
        tag = ""
        conteo = 0
        for i in value:
            # tag = i[0][0]
            if i[0][0] == tag:
                conteo += 1
                if conteo < 10:
                    yield i[0], i[1]

            else:
                tag = i[0][0]
                conteo = 0
                yield i[0], i[1]


if __name__ == "__main__":
    top_10_words_per_tag.run()
