# AuntISARA

A python based Soft IOC Server for ISARA Automounter

## Installation

1.  Create a directory for the IOC instance and place your positions file in it.
2.  Install the package in a virtual environment:

    ```bash
    git clone https://github.com/michel4j/aunt-isara.git
    python -m venv isara
    source isara/bin/activate
    pip install /path/to/aunt-isara
    ```

3.  Run the application to connect to the ISARA controller:

    ```bash
    mkdir isara-config && cd isara-config
    isara-server --device ISARA-MX1 --address <IP-ADDRESS> --commands 10000 --status 1000
    ```
    Change the device prefix to a meaningful name. This will be the prefix for all your EPICS process variables.

4.  Launch the User Interface. The included GUI file is a GtkDM file. You can install GtkDM as follows:

    ```bash
    pip install gtkdm
    gtkdm -m "robot=ISARA-MX1" /path/to/aunt-isara/deploy/isara.ui
    ```

## Post-Installation Steps

Once the Automounter is running, you must define some positions. Positions are important because some commands can only be run from specified positions. Newer versions of the robot internally enforce positions but this system an additional layer of protections. At a minium, 'SOAK' and 'HOME' positions must be defined. It is recommended to defined various 'DRY_XXX' positions also.

To define a position, manually move the robot to that position, and type in the position name, and tolerance and then click the save button on the operator screen. The tolerance determines how sensitive the robot should be at that position. Positions can be replaced by toggling the "Overwrite Position" to ON before saving the position.

For positions like DRY that have multiple sub-positions, use DRY as the prefix and save each one with a separate suffix. Also, there is a different HOME position for each tool so you must save them separately always starting with the HOME prefix.