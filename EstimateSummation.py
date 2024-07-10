from mpi4py import MPI
import csv
import sys
import time

# read csv data
def read_csv(filename):
    data = []
    with open(filename, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            data.append(row)
    return data

filename = "../../migration.csv"

# create connection to file
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

if rank == 0:
    data = read_csv(filename)
    chunks = [[] for _ in range(size)]
    for i, row in enumerate(data):
        chunks[i % size].append(row)
else:
    chunks = None

# start timer
start_time = time.time()
# Scatter chunks to all processes
local_chunk = comm.scatter(chunks, root=0)
print("Process", rank, "received", len(local_chunk), "rows of data")

# Perform local computation (sum of 'estimate' column in the chunk)
local_sum = sum(int(row['estimate']) for row in local_chunk)

# Gather local sums to root process
global_sums = comm.gather(local_sum, root=0)

# Compute total sum in root process
if rank == 0:
    total_sum = sum(global_sums)
    # end timer
    end_time = time.time()

    print("Sum of 'estimate' column:", total_sum)
    print("Time taken for sum calculation: ", (end_time - start_time) * 1000, " milliseconds")
