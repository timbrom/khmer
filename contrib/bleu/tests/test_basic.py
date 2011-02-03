from cStringIO import StringIO
from utils import run_bleu, get_partition
from screed.fasta import fasta_iter

def test_single():
    results = run_bleu('data/single.fa', 32, int(1e6))
    results = StringIO(results)
    
    records = list(fasta_iter(results, parse_description=False))
    assert len(records) == 1
    
    record = records[0]
    _, partition = record['name'].rsplit('\t', 1)
    assert int(partition) == 0, results
    
def test_multiple():
    results = run_bleu('data/multiple.fa', 32, int(1e6))
    results = StringIO(results)
    
    records = list(fasta_iter(results, parse_description=False))
    assert len(records) == 4

    partitions = [ get_partition(r['name']) for r in records ]
    
    assert partitions[0] == partitions[1], partitions
    assert partitions[2] == partitions[3], partitions

def test_20_a_k19():                    # should all connect at 19
    results = run_bleu('data/random-20-a.fa', 19, int(1e8))
    results = StringIO(results)
    
    records = list(fasta_iter(results, parse_description=False))
    partitions = ( get_partition(r['name']) for r in records )
    partitions = ( p for p in partitions if p > 0 )
    partitions = set(partitions)

    assert len(partitions) == 1
    
def test_20_a_k20():
    results = run_bleu('data/random-20-a.fa', 20, int(1e8))
    results = StringIO(results)
    
    records = list(fasta_iter(results, parse_description=False))
    partitions = ( get_partition(r['name']) for r in records )
    partitions = ( p for p in partitions if p > 0 )
    partitions = set(partitions)

    assert len(partitions) == 0
    
def test_20_b_k19():                    # should all connect at 19
    results = run_bleu('data/random-20-b.fa', 19, int(1e8))
    results = StringIO(results)
    
    records = list(fasta_iter(results, parse_description=False))
    partitions = ( get_partition(r['name']) for r in records )
    partitions = ( p for p in partitions if p > 0 )
    partitions = set(partitions)

    assert len(partitions) == 1
    
def test_20_b_k20():
    results = run_bleu('data/random-20-b.fa', 20, int(1e8))
    results = StringIO(results)
    
    records = list(fasta_iter(results, parse_description=False))
    partitions = ( get_partition(r['name']) for r in records )
    partitions = ( p for p in partitions if p > 0 )
    partitions = set(partitions)

    assert len(partitions) == 0

def test_32_c_k30():                    # should all connect at 30
    results = run_bleu('data/random-31-c.fa', 30, int(1e8))
    results = StringIO(results)
    
    records = list(fasta_iter(results, parse_description=False))
    partitions = ( get_partition(r['name']) for r in records )
    partitions = ( p for p in partitions if p > 0 )
    partitions = set(partitions)

    assert len(partitions) == 1
    
def test_32_c_k31():                    # ...but not at 31.
    results = run_bleu('data/random-31-c.fa', 31, int(1e8))
    results = StringIO(results)
    
    records = list(fasta_iter(results, parse_description=False))
    partitions = ( get_partition(r['name']) for r in records )
    partitions = ( p for p in partitions if p > 0 )
    partitions = set(partitions)

    assert len(partitions) == 0
