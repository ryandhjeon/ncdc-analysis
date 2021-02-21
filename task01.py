from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime, timedelta


class MRTask01(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper1)
                   # reducer=self.reducer1),
            # MRStep(reducer=self.reducer2)
        ]

    def mapper1(self, key, line):
        values = line.split()
        try:
            base = int(values[0])
            ymd = values[1]
            yield key, ymd

        except ValueError:
            pass

    def reducer1(self, key, value):
        # date1 = datetime(day=1, month=1, year=1903)
        # date2 = datetime(day=1, month=1, year=1904)
        # diff = (date2 - date1)
        # diffYear = int(date2.strftime("%Y")) - int(date1.strftime("%Y"))
        # val = (0.8 * diff.days)

        dayList = list(set(list(value))) # Get days in a list

        yearList = []
        yearCount = []

        # if (len(dayList) >= val):
        for i in dayList:
            yearList.append(i)

        yield key, len(dayList)

        # yearlist2 = list(set(yearList))
        # if (len(yearlist2) == diffYear):
        #     yield key, len(yearlist2)
        # else:
        #     # yield key, len(yearlist2)
        #     yield key, diffYear



if __name__ == '__main__':
    MRTask01.run()
