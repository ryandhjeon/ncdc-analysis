from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


class MRTask01(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   reducer=self.reducer1),
            MRStep(reducer=self.reducer2)
        ]

    def mapper1(self, _, line):
        values = line.split()
        try:
            base = int(values[0])
            year = int(values[2])
            if (year >= 1920 and year <= 1940):
                yield base, year
        except ValueError:
            pass

    def reducer1(self, key, value):
        year = list(value)
        year_list = []
        for i in year:
            year_list.append(i)
        year_set_list = list(set(year_list))

        date1 = datetime(day=1, month=1, year=1920)
        date2 = datetime(day=1, month=1, year=1941)
        diff_year = int(date2.strftime("%Y")) - int(date1.strftime("%Y"))
        val = (0.8 * diff_year)

        if len(year_set_list) >= val:
            yield None, key

        if len(year_set_list) >= diff_year:
            yield key, None

        year_count = int(len(year_set_list))
        yield None, (year_count, key, year_set_list)


    def reducer2(self, _, values):
        count = 0
        for year_count, key, year_set_list in sorted(values, reverse=True):
            count += 1
            if count <= 50:
                yield (year_count, key), year_set_list


if __name__ == '__main__':
    MRTask01.run()
