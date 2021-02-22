from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime, timedelta


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
            id = int(values[0])
            year = int(values[2][0:4])
            if (year >= 1920 and year <= 1940):
                yield id, year
        except ValueError:
            pass

    def reducer1(self, key, values):
        year = list(values)
        yearList = []
        for y in year:
            yearList.append(y)
        yr = list(set(yearList))
        date1 = datetime(day=1, month=1, year=1920)
        date2 = datetime(day=1, month=1, year=1941)
        diffYear = int(date2.strftime("%Y")) - int(date1.strftime("%Y"))
        operable = (0.8 * diffYear)

        if (len(yr) >= operable):
            yield "Operable for 80% or more of the entire time period 1920-1940: ", key

        if (len(yr) >= diffYear):
            yield key, "was operable all years in the time period 1920-1940"

        yrCount = int(len(yr))
        yield None, (yrCount, key, yr)

    def reducer2(self, _, values):
        count = 0
        for yrCount, key, yr in sorted(values, reverse=True):
            count += 1
            if count < 51:
                yield (yrCount, key), yr


if __name__ == '__main__':
    MRTask01.run()