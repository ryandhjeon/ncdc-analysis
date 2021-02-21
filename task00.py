from mrjob.job import MRJob
from mrjob.step import MRStep


class MRFilterData(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   reducer=self.reducer1),
            MRStep(reducer=self.reducer2)
        ]

    def mapper1(self, _, line):
        values = line.split()

        try:
            id = int(values[0])
            years = values[2][0:4]
            month = values[2][4:6]
            temp = float(values[21])

            yield (id, years, month), temp

        except ValueError:
            pass

    def reducer1(self, key, values):
        counts = 0
        temp_list = []
        id = key[0]
        year = key[1]
        month = key[2]

        for i in values:
            counts += 1
            temp_list.append(i)

        if counts >= 2:
            yield (id, year), (month, temp_list)

    def reducer2(self, key, pairs):
        pair_list = list(pairs)
        id = key[0]
        year = int(key[1])

        if len(pair_list) == 12:
            for pl in pair_list:
                month = int(pl[0])
                for p in pl[1]:
                    yield id, (id, year, month, p)


if __name__ == '__main__':
    MRFilterData.run()