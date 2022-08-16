import re

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRLessViewed(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1)]

    def mapper1(self, _, line):
        regex_id = r"\sId=(\S+)"
        regex_vcount = r"\sViewCount=(\S+)"
        d_id = re.search(regex_id, line)
        d_vcount = re.search(regex_vcount, line)

        post_id = d_id.group(1) if d_id else ""
        post_vcount = int(d_vcount.group(1).replace('"', "")) if d_vcount else 0

        if post_vcount != 0:
            yield None, {"id": post_id, "value": post_vcount}

    def reducer1(self, _, values):
        data = list(values)
        data.sort(key=lambda x: x.get("value"))
        data.reverse()
        data = data[:10]
        for item in data:
            yield item.values()


if __name__ == "__main__":
    MRLessViewed.run()
