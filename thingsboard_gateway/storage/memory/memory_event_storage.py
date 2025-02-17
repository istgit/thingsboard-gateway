#     Copyright 2024. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

# Responsible for temporarily storing events in memory.
# Queue, Empty and Full are imported from queue module which is standard library for handlings queues. A queue is a
# data structure where elements are added to the back and removed from the front
# EventStorage is a base class that MemoryEventStorage is inheriting

# __init__ Method
# init method is the constructor method that runs when a MemoryEventStorage object is created
# Sets up a queue to hold events with a max size queue len and how many events to read at once
# initializes an empty list for empty pack to temporarily hold events while there are being processed and a flag to
# indicate whether the storage is active or stopped

# put Method
# put method attempts to add an event to the queue
# if the queue is full or storage is stopped it logs an error
# returns true if the event was successfully added

# get_event_pack Method
# get_event_pack retrieves a bunch of events from the queue for processing
# if the event_pack list is empty it fills it with up to events_per_time events from the queue
# returns the event_pack list which contains events to be processed

# event_pack_processing_done Method
# this method clears the event_pack list after events have been processed, making it ready for the next batch

# stop Method
# sets the stopped flag to true indicating no more events should be added to the storage

# len Method
# Returns number of events currently in queue

from queue import Empty, Full, Queue

from thingsboard_gateway.storage.event_storage import EventStorage, log


class MemoryEventStorage(EventStorage):
    def __init__(self, config):
        self.__queue_len = config.get("max_records_count", 10000)
        self.__events_per_time = config.get("read_records_count", 1000)
        self.__events_queue = Queue(self.__queue_len)
        self.__event_pack = []
        self.__stopped = False
        log.debug("Memory storage created with following configuration: \nMax size: %i\n Read records per time: %i",
                  self.__queue_len, self.__events_per_time)

    def put(self, event):
        success = False
        if not self.__stopped:
            try:
                self.__events_queue.put_nowait(event)
                success = True
            except Full:
                log.error("Memory storage is full!")
        else:
            log.error("Storage is stopped!")
        return success

    def get_event_pack(self):
        try:
            if not self.__event_pack:
                self.__event_pack = [self.__events_queue.get_nowait() for _ in
                                     range(min(self.__events_per_time, self.__events_queue.qsize()))]
        except Empty:
            pass
        return self.__event_pack

    def event_pack_processing_done(self):
        self.__event_pack = []

    def stop(self):
        self.__stopped = True

    def len(self):
        return self.__events_queue.qsize()
