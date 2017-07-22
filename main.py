#!/usr/bin/python3
import copy
import os
import queue
import sys
import threading
import time



class worker( threading.Thread):
    def __init__(self, thread_id, running=None):
        super().__init__(name="worker-thread-"+str(thread_id))
        self._pending = queue.Queue()

        self.running = running

        self.id = thread_id

        return

    def run( self):
        print("        => starting worker: "+str(self.id))
        while self.running.is_set():
            try:
                nextMessage = self._pending.get( True, 1.0)
                if nextMessage:
                    print("        => {}: {}".format( self.name, nextMessage))
            except queue.Empty:
                # routine occurence. ignore
                continue



class coordinator( threading.Thread):
    def __init__(self):
        super().__init__(name="coordinator-thread")
        self.workers = []

        self.running = threading.Event()
        self.running.set()

        self.workers.append( worker( 1, self.running))
        self.workers.append( worker( 2, self.running))

    def enqueue( self, msg ):
        for worker in self.workers:
            try:
                worker._pending.put_nowait(copy.copy(msg))
            except QueueFull as fullError:
                print("thread queue is full.  this is an error")
                # log error here;


    def halt( self):
        self.running.clear()
        return self

    def run( self):
        print("    >> starting workers.")
        for worker in self.workers:
            worker.start()

        print("    >> sending messages.")
        time.sleep(1.5)
        self.enqueue( "msg1")
        print("    >> msg1 end.")
        # time.sleep(1.5)
        # self.enqueue( "msg2")
        # print("    >> msg2 end.")
        # time.sleep(1.5)
        # self.enqueue( "msg3")
        # print("    >> msg3 end.")

        time.sleep(1.0)
        print("    >> Halting all threads.")
        self.halt()

        print("    >> Rejoining.")
        for worker in self.workers:
            worker.join()

        print("    >> coordinator 'run' finished.")



if __name__ == "__main__":
    print("> Starting...")
    cdr = coordinator()
    cdr.start()
    cdr.join()
    print("> Finished.")
