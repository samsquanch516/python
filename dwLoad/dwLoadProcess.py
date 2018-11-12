#!/usr/bin/python
import DataPull, random
from threading import Thread
from queue import Queue


def myFunc(worker, x):
    print("Starting: ")
    dataPull = DataPull.DataPull()
    dataPull.start_data_pull(worker, x)
    print("complete: " + worker)


# The threader thread pulls an worker from the queue and processes it
def threader():
    while True:
        # gets an worker from the queue
        worker = q.get()

        # Run the example job with the avail worker in queue (thread)
        myFunc(worker, random.randint(1,10))

        # completed with the job
        q.task_done()


def main():
    # define data loads
    workloads = ['inventory_dimension', 'dealer_dimension', 'user_dimension','price_dimension','listing_dimension']

    for x in range(q.maxsize):
        t = Thread(target=threader)

        # classifying as a daemon, so they will die when the main dies
        t.daemon = True

        # begins, must come after daemon definition
        t.start()

    for worker in workloads:
        q.put(worker)

    # wait until the thread terminates.
    q.join()

if __name__ == "__main__":
    # define the queue and the maximum number of running threads
    q = Queue(maxsize=4)

    #initate program
    main()
