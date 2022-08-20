import re

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRLessViewed(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        regex_id = r"\sId=(\S+)"
        regex_vcount = r"\sViewCount=(\S+)"
        mo_id = re.search(regex_id, line)
        mo_vcount = re.search(regex_vcount, line)

        post_id = mo_id.group(1) if mo_id else ""
        post_vcount = int(mo_vcount.group(1).replace('"', "")) if mo_vcount else 0

        if post_vcount != 0:
            yield None, {"id": post_id, "count": post_vcount}

    def reducer(self, _, values):
        data = list(values)
        data.sort(key=lambda x: x.get("count"))
        data = data[:10]
        for item in data:
            yield item.values()


if __name__ == "__main__":
    MRLessViewed.run()
