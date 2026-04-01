from pydnp3 import opendnp3
import logging

# Get logger
logger = logging.getLogger("DNP3Visitor")


class VisitorIndexedBinary(opendnp3.IVisitorIndexedBinary):
    """Visitor for Binary Input data (Group 1) and Binary Input Events (Group 2)"""

    def __init__(self):
        super().__init__()
        self.index_and_value = []

    def OnValue(self, indexed_binary):
        try:
            index = indexed_binary.index
            value = indexed_binary.value.value
            # Try to get timestamp - binary events (Group2) have time, static (Group1) may not
            event_time = None
            if hasattr(indexed_binary.value, 'time'):
                time_obj = indexed_binary.value.time
                if hasattr(time_obj, 'value'):
                    event_time = time_obj.value
                else:
                    event_time = time_obj

            logger.debug("=" * 50)
            logger.debug(f"[VisitorIndexedBinary] Processing")
            logger.debug(f"  Index: {index}")
            logger.debug(f"  Value: {value}")
            logger.debug(f"  Timestamp: {event_time}")
            logger.debug("=" * 50)

            self.index_and_value.append((index, value, event_time))
        except Exception as e:
            logger.error(f"[VisitorIndexedBinary] Error: {e}")
            import traceback
            traceback.print_exc()


class VisitorIndexedDoubleBitBinary(opendnp3.IVisitorIndexedDoubleBitBinary):
    def __init__(self):
        super().__init__()
        self.index_and_value = []

    def OnValue(self, indexed):
        try:
            index = indexed.index
            value = indexed.value.value
            event_time = None
            if hasattr(indexed.value, 'time'):
                time_obj = indexed.value.time
                event_time = getattr(time_obj, 'value', time_obj)

            logger.debug(f"[VisitorIndexedDoubleBitBinary] index={index}, value={value}, time={event_time}")
            self.index_and_value.append((index, value, event_time))
        except Exception as e:
            logger.error(f"[VisitorIndexedDoubleBitBinary] Error: {e}")


class VisitorIndexedCounter(opendnp3.IVisitorIndexedCounter):
    def __init__(self):
        super().__init__()
        self.index_and_value = []

    def OnValue(self, indexed):
        try:
            index = indexed.index
            value = indexed.value.value
            event_time = None
            if hasattr(indexed.value, 'time'):
                time_obj = indexed.value.time
                event_time = getattr(time_obj, 'value', time_obj)

            logger.debug(f"[VisitorIndexedCounter] index={index}, value={value}, time={event_time}")
            self.index_and_value.append((index, value, event_time))
        except Exception as e:
            logger.error(f"[VisitorIndexedCounter] Error: {e}")


class VisitorIndexedFrozenCounter(opendnp3.IVisitorIndexedFrozenCounter):
    def __init__(self):
        super().__init__()
        self.index_and_value = []

    def OnValue(self, indexed):
        try:
            index = indexed.index
            value = indexed.value.value
            event_time = None
            if hasattr(indexed.value, 'time'):
                time_obj = indexed.value.time
                event_time = getattr(time_obj, 'value', time_obj)

            logger.debug(f"[VisitorIndexedFrozenCounter] index={index}, value={value}, time={event_time}")
            self.index_and_value.append((index, value, event_time))
        except Exception as e:
            logger.error(f"[VisitorIndexedFrozenCounter] Error: {e}")


class VisitorIndexedBinaryOutputStatus(opendnp3.IVisitorIndexedBinaryOutputStatus):
    def __init__(self):
        super().__init__()
        self.index_and_value = []

    def OnValue(self, indexed):
        try:
            index = indexed.index
            value = indexed.value.value
            event_time = None
            if hasattr(indexed.value, 'time'):
                time_obj = indexed.value.time
                event_time = getattr(time_obj, 'value', time_obj)

            logger.debug(f"[VisitorIndexedBinaryOutputStatus] index={index}, value={value}, time={event_time}")
            self.index_and_value.append((index, value, event_time))
        except Exception as e:
            logger.error(f"[VisitorIndexedBinaryOutputStatus] Error: {e}")


class VisitorIndexedAnalogOutputStatus(opendnp3.IVisitorIndexedAnalogOutputStatus):
    def __init__(self):
        super().__init__()
        self.index_and_value = []

    def OnValue(self, indexed):
        try:
            index = indexed.index
            value = indexed.value.value
            event_time = None
            if hasattr(indexed.value, 'time'):
                time_obj = indexed.value.time
                event_time = getattr(time_obj, 'value', time_obj)

            logger.debug(f"[VisitorIndexedAnalogOutputStatus] index={index}, value={value}, time={event_time}")
            self.index_and_value.append((index, value, event_time))
        except Exception as e:
            logger.error(f"[VisitorIndexedAnalogOutputStatus] Error: {e}")


class VisitorIndexedTimeAndInterval(opendnp3.IVisitorIndexedTimeAndInterval):
    def __init__(self):
        super().__init__()
        self.index_and_value = []

    def OnValue(self, indexed):
        try:
            index = indexed.index
            value = indexed.value
            logger.debug(f"[VisitorIndexedTimeAndInterval] index={index}, value={value}")
            self.index_and_value.append((index, value, None))
        except Exception as e:
            logger.error(f"[VisitorIndexedTimeAndInterval] Error: {e}")


class VisitorIndexedAnalogTime(opendnp3.IVisitorIndexedAnalog):
    """Visitor for indexed analog values with timestamp support"""

    def __init__(self):
        super().__init__()
        self.index_and_value = []

    def OnValue(self, indexed_analog):
        try:
            index = indexed_analog.index
            value = indexed_analog.value.value
            # Extract timestamp if available
            event_time = None
            if hasattr(indexed_analog.value, 'time'):
                time_obj = indexed_analog.value.time
                if hasattr(time_obj, 'value'):
                    event_time = time_obj.value
                else:
                    event_time = time_obj

            logger.debug(f"[VisitorIndexedAnalogTime] index={index}, value={value}, time={event_time}")
            self.index_and_value.append((index, value, event_time))
        except Exception as e:
            logger.error(f"[VisitorIndexedAnalogTime] Error: {e}")