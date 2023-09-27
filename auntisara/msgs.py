import re
from enum import IntEnum, IntFlag, auto


class StatusType(IntEnum):
    IDLE, WAITING, BUSY, STANDBY, FAULT = range(5)

class Error(IntFlag):
    BRAKE_TOGGLED = auto()
    EMERGENCY_STOP = auto()
    COLLISION = auto()
    COMMUNICATION_ERROR = auto()
    WRONG_MENU = auto()
    WRONG_MODE = auto()
    AWAITING_GONIO = auto()
    AWAITING_SAMPLE = auto()
    AWAITING_PUCK = auto()
    AWAITING_FILL = auto()
    AWAITING_LID = auto()
    SAMPLE_MISMATCH = auto()


MESSAGES = [
    {
        "error": "Manual brake control selected",
        "flag": Error.BRAKE_TOGGLED
    },
    {
        "error": "emergency stop or air pressure fault",
        "flag": Error.EMERGENCY_STOP
    },
    {
        "error": "Modbus communication fault",
        "flag": Error.COMMUNICATION_ERROR
    },
    {
        "error": "LOC menu not disabled",
        "flag": Error.WRONG_MENU,
    },
    {
        "error": "Remote Mode requested",
        "flag": Error.WRONG_MODE
    },
    {
        "error": "collision",
        "flag": Error.COLLISION,
    },
    {
        "error": "WAIT for RdTrsf condition",
        "flag": Error.AWAITING_GONIO,
    },
    {
        "error": "WAIT for SplOn condition",
        "flag": Error.AWAITING_SAMPLE,
    },
    {
        "error": "WAIT for CassOK condition",
        "flag": Error.AWAITING_PUCK,
    },
    {
        "error": "No LN2 available, regulation stopped",
        "flag": Error.COLLISION,
    },
    {
        "error": "collision at the gonio",
        "flag": Error.COLLISION,
    },
    {
        "error": "Lid",
        "flag": Error.AWAITING_LID,
    }
]


def parse_error(message):
    """
    Parse an error message into a warning text and a help text and error code bit

    :param message: Message from the status port, return value of the 'message' command
    :return: (description, help, bit) a tuple of strings
    """
    flag = Error(0)

    for info in MESSAGES:
        if re.search(info['error'], message):
            flag |= info['flag']
            break

    return flag
