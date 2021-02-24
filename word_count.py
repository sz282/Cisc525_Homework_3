#!/usr/bin/python3

from pyspark import SparkContext, SparkConf
import string
import sys
import subprocess

class Word_Count():
    def __init__(self):
        self.conf = SparkConf().setAppName('Word Count App')
        self.sc=SparkContext(conf=self.conf)
        
    def load_text(self, text_file):
        self.text = self.sc.textFile(text_file)
        
    def counts(self, out_dir):
        self.counts = self.text\
            .flatMap(lambda line: strip_punc(line))\
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b)
        self.counts.saveAsTextFile(out_dir)

    def search_counts(self, word):
        for count in self.counts.collect():
            # kv = str(count).translate(str.maketrans('', '', string.punctuation)).split(' ')
            kv = strip_punc(str(count))
            if word == kv[0]:
                print('Found \'{}\' occurs \'{}\' times'.format(kv[0], kv[1]))
                self.search_word_in_line(word)
                break

    def search_word_in_line(self, word):
        line_no = 1
        for line in self.text.collect():
            if word in strip_punc(line):
                print('{}. {}'.format(line_no, line))
            line_no += 1

            
def strip_punc(s):
    return s.translate(str.maketrans('', '', string.punctuation)).split(' ')


def delete_out_dir(out_dir):
    subprocess.call(["hdfs", "dfs", "-rm", "-R", out_dir])


def main(argv):
    delete_out_dir(argv[1])
    word_count = Word_Count()
    word_count.load_text(argv[0])
    word_count.counts(argv[1])
    word_count.search_counts(argv[2])
    
if __name__ == '__main__':
    main(sys.argv[1:])
    