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
            id = values[0]
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
        p_list = list(pairs)
        id = key[0]
        year = key[1]

        if len(p_list) == 12:
            for i in p_list:
                month = i[0]
                for temp in i[1]:
                    yield id, (int(id), int(year), int(month), float(temp))


if __name__ == '__main__':
    MRFilterData.run()