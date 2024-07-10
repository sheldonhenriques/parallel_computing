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

# create connection to MPI
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

# Perform local computation (count occurrences of each country)
local_counts = {}
for row in local_chunk:
    country = row['country_of_residence']
    if country in local_counts:
        local_counts[country] += 1
    else:
        local_counts[country] = 1

# Gather local counts to root process
global_counts = comm.gather(local_counts, root=0)

# Compute total counts in root process
if rank == 0:
    total_counts = {}
    for counts in global_counts:
        for country, count in counts.items():
            if country in total_counts:
                total_counts[country] += count
            else:
                total_counts[country] = count
    # end timer
    end_time = time.time()

    print("Counts of countries:")
    for country, count in total_counts.items():
        print(country, ":", count)

    print("Time taken for counting: ", (end_time - start_time) * 1000, " milliseconds")
