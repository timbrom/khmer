import khmer, sys
import gc
import glob

K = 32

filenames=sys.argv[1:]
subset_filenames=glob.glob('*.pmap')

def load(filename, ht):
    pmap_filename = filename
    print 'loading', filename
    subset = ht.load_subset_partitionmap(pmap_filename)
    print ht.subset_count_partitions(subset)
    print 'merging', filename
    ht.merge_subset(subset)

# create a fake-ish ht; K matters, but not hashtable size.
ht = khmer.new_hashbits(32, 1, 1)

# detect all of the relevant partitionmap files
#subset_filenames = glob.glob(filename + '.subset.*.pmap')

# load & merge
for subset_file in subset_filenames:
    print '<-', subset_file
    load(subset_file, ht)

# partition!
for filename in filenames:
    ht.output_partitions(filename, filename + '.part')

print ht.count_partitions()
