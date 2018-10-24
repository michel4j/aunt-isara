import re


MESSAGES = {
    0: {
        "error": "doors opened",
        "description": "Doors opened",
        "help": "Close the door or switch to Manual mode"
    },
    1: {
        "error": "Manual brake control selected",
        "description": "Manual brake button is not on the 0 position",
        "help": "Turn the manual brake dial to the 0 position"
    },
    2: {
        "error": "emergency stop or air pressure fault",
        "description": "Emergency stop pressed or the compressed air pressure too low",
        "help": "Inspect the robot, eliminate the risk and turn-off the emergency stop or check air pressure"
    },
    3: {
        "error": "collision detection",
        "description": "The robot collided with something",
        "help": ("Abort the current task; safely unlock the brakes and move the robot manually; "
                 "re-engage the shock detector and run the 'safe' command")
    },
    4: {
        "error": "Modbus communication fault",
        "description": "Internal communication fault",
        "help": ("Inspect the ethernet cable connections inside the electro-pneumatic rack, "
                 "and check the power of the CS8C controller and the PLC")
    },
    5: {
        "error": "LOC menu not disabled",
        "description": "The current menu on the teach pendant is 'LOC'",
        "help": "Quit the 'LOC' menu on the Teach Pendant"
    },
    6: {
        "error": "Remote Mode requested",
        "description": "Remote mode is not selected",
        "help": "Switch to remote mode"
    },
    7: {
        "error": "Disable when path is running",
        "description": "A command was sent while already processing a task",
        "help": "Wait for current task to complete"
    },
    8: {
        "error": "X- collision",
        "description": "X- limit reached",
        "help": "Modify the trajectory or adjust the limit"
    },
    9: {
        "error": "X+ collision",
        "description": "X+ limit reached",
        "help": "Modify the trajectory or adjust the limit"
    },
    10: {
        "error": "Y- collision",
        "description": "Y- limit reached",
        "help": "Modify the trajectory or adjust the limit"
    },
    11: {
        "error": "Y+ collision",
        "description": "Y+ limit reached",
        "help": "Modify the trajectory or adjust the limit"
    },
    12: {
        "error": "Z- collision",
        "description": "Z- limit reached",
        "help": "Modify the trajectory or adjust the limit"
    },
    13: {
        "error": "Z+ collision",
        "description": "Z+ limit reached",
        "help": "Modify the trajectory or adjust the limit"
    },
    14: {
        "error": "Robot foot collision",
        "description": "Collision with the foot of the robot arm",
        "help": "Modify the trajectory"
    },
    15: {
        "error": "WAIT for .* condition",
        "description": "Waiting for required condition to go to the next point",
        "help": "Inspect the sample environment and generated input signals"
    },
    16: {
        "error": "low level alarm",
        "description": "LN2 level in the Dewar is low",
        "help": "Check the LN2 supply"
    },
    17: {
        "error": "high level alarm",
        "description": "LN2 level in the Dewar is too high",
        "help": "Close the main valve of the LN2 supply and contact IRELEC support"
    },
    18: {
        "error": "No LN2 available, regulation stopped",
        "description": "No LN2 available, autofill stopped",
        "help": "Check LN2 main supply, check phase sensor, and contact IRELEC support"
    },
    19: {
        "error": "FillingUp Timeout",
        "description": "Maximum time for filling up was exceeded",
        "help": "Check LN2 main supply, check level sensor, and contact IRELEC support"
    },
}


def parse_error(message):
    """
    Parse an error message into a warning text and a help text and error code bit

    :param message: Message from the status port, return value of the 'message' command
    :return: (description, help, bit) a tuple of strings
    """

    for bit, info in MESSAGES.items():
        if re.search(info['error'], message):
            return info['description'], info['help'], bit

    return "", "", None
