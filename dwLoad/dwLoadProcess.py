import DataPull
import Dimension
from threading import Thread
from Queue import Queue


def myFunc(worker):
    print("Starting: ")
    dataPull = DataPull.DataPull()
    dataPull.start_data_pull(worker.get_view(), worker.get_schema())
    print("complete: " + worker.get_view())


# The threader thread pulls an worker from the queue and processes it
def threader():
    while True:
        # gets an worker from the queue
        worker = q.get()

        # Run the example job with the avail worker in queue (thread)
        myFunc(worker)

        # completed with the job
        q.task_done()


def main():
    inventory = Dimension.Dimension("inventory_dimension", "history", "inventory_dimension")
    ecom = Dimension.Dimension("ecom_dimension","history","ecom_dimension")
    # define data loads
    workloads = [inventory,ecom]

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

    return 0


if __name__ == "__main__":
    # define the queue and the maximum number of running threads
    q = Queue(maxsize=4)

    #initate program
    main()
