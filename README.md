
AuntISARA
=========

A python based Soft IOC Server for ISARA Automounter

Usage
=====
In order to use "AuntISARA", you need have a functioning install of python-softdev and its requirements and procServ.
 
1. Create a directory for the IOC instance. The directory should be named exactly like the device name but the location
   is irelevant. 
2. Copy the init-template file to /etc/init.d and rename it as appropriate.
3. Edit the file from (2) above to reflect your environment and to set all the required instance parameters. Specifically,
   change the device name to a meaningful name.
4. Enable the init file using your system commands. For example, `systemctl enable <init-file-name>`.
5. Start the init file using your system commands. For example `systemctl start <init-file-name>`.
You can manage the instance daemon through procServ, by telneting to the configured port. 
6. The operator screen can be launched using the script `bin/runOpCtrl`. This script takes a single parameter which is the device name provided in step (3). See the `opi/isara-operator-screen.pdf`. 
7. Once the Automounter is running, you must define some positions. Positions are important because commands are only allowed to run from Minimum required are 'SOAK' and 'HOME' for most commands but it is recommended to defined various 'DRY_XXX' positions. To define a position, manually move the robot to that position using the pendent, and type in the position name, and tolerance and then click the save button on the operator screen.  The tolerance determines how sensitive the robot should be at that position.  Positions can be replaced by toggling the "Overwrite Position"  to ON before saving the position. For positions like DRY that have multiple sub-positions, use DRY as the prefix and save each one with a separate suffix.  Also, there is a different HOME position for each tool so you must save them separately always starting with the HOME prefix.

