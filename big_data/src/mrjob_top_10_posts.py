from mrjob.job import MRJob
from mrjob.step import MRStep


class top_10_most_viewed_posts(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_posts,
                combiner=self.combiner_posts,
                reducer=self.reducer_find_top_10_posts,
            )
        ]

    def mapper_get_posts(self, _, line):
        # yield each word in the line
        if line.find("ViewCount") != -1:

            key_split = line.split("<row ")
            key_split2 = key_split[1].split(" Post")
            key = key_split2[0]

            value_split = line.split('ViewCount="')
            value_split2 = value_split[1].split('" Body')
            value = int(value_split2[0])
            yield (key, value)

    def combiner_posts(self, key, value):
        # optimization: sum the words we've seen so far

        yield None, {"ID": key, "value": sum(value)}

    def reducer_find_top_10_posts(self, _, value):
        lista = list(value)
        lista.sort(key=lambda x: x.get("value"))
        lista.reverse()
        lista = lista[:10]
        for i in lista:
            yield i.values()


if __name__ == "__main__":
    top_10_most_viewed_posts.run()
