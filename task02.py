from mrjob.job import MRJob
import statistics

class MRTask02(MRJob):
    def mapper(self, _, line):
        values = line.split()
        try:
            year = int(values[2][0:4])
            temp = float(values[4][0:4])
            yield year, temp
        except ValueError:
            pass

    def reducer(self, year, temp):
        temp_list = list(temp)
        min_temp = min(temp_list)
        max_temp = max(temp_list)
        avg_temp = statistics.mean(temp_list)
        median_temp = statistics.median(temp_list)
        yield None, (year, min_temp, max_temp, avg_temp, median_temp)

if __name__ == '__main__':
    MRTask02.run()
