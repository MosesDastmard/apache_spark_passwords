import numpy as np
import argparse
import progressbar
import pyspark
from pyspark.sql import SparkSession

CHARACTERS = list('qwertyuiop[]asdfghjkl;zxcvbnm,.1234567890!@#$%^&*()QWERTYUIOP{}ASDFGHJKL:ZXCVBNM<>')


def gen_pass(n=10):
    return "".join(np.random.choice(CHARACTERS, n, True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('N', type=str,
                        help="number of passwords")
    parser.add_argument('n', type=str,
                        help="password length")
    parser.add_argument('output_path', type=str,
                        help="output path")
    parser.add_argument('mode', type=str,
                        help="spark/python")

    args = parser.parse_args()

    N = int(args.N)
    n = int(args.n)
    output_path = args.output_path
    mode = args.mode
    if mode != 'spark':
        with open(output_path, 'w') as f:
            for _ in progressbar.progressbar(range(N)):
                f.write(gen_pass(n) + '\n')
    else:
        spark = SparkSession.builder.appName('Spark Password Generator').getOrCreate()
        sparkContext = spark.sparkContext
        sparkContext.parallelize([n]*N, 64).map(gen_pass).saveAsTextFile(output_path) #.repartition(64).coalesce(64)
    print("{} passwords generated randomly in {}".format(N, output_path))
