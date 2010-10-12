#! /usr/bin/env python
import sys
import khmer
from screed.fasta import fasta_iter

K = 32

###

def get_partition(record):
    pid = record['name'].rsplit('\t', 1)[1]
    return int(pid)

###

ht = khmer.new_hashbits(K, 1, 1)

filenames = sys.argv[1:]

for filename in filenames:
    ht.consume_partitioned_fasta(filename)

before = ht.count_partitions()

for filename in filenames:
    last_name = None
    last_record = None

    seqfile = fasta_iter(open(filename), parse_description=False)
    for n, record in enumerate(seqfile):
        if n % 10000 == 0:
            print '...', n

        name = record['name'].split()[0]
        name = name.split('/', 1)[0]

        if name == last_name:
            if 1:
                pid_1 = ht.get_partition_id(last_record['sequence'][:K])
                pid_2 = ht.get_partition_id(record['sequence'][:K])

                ht.join_partitions(pid_1, pid_2)
#            else:                           # TEST
#                pid_1 = get_partition(last_record)
#                pid_2 = get_partition(record)
#                assert pid_1 == pid_2, (last_record, record, pid_1, pid_2)

        last_name = name
        last_record = record

for filename in filenames:
    ht.output_partitions(filename, filename + '.paired')

print 'before:', before
after = ht.count_partitions()
print 'after:', after

n_combined = before[0] - after[0]
print 'combined:', n_combined
