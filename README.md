
AuntISARA
=========

A python based Soft IOC Server for ISARA Automounter

Usage
=====
In order to use "AuntISARA", you need have a functioning install of python-softdev and its requirements and procServ.
 
1. Create a directory for the IOC instance. The directory should be named exactly like the device name but the location
   is irelevant. 
2. Copy the init-template file to /etc/init.d and rename it as appropriate.
3. Edit the file from (2) above to reflect your environment and to set all the required instance parameters
4. Enable the init file using your system commands. For example, `systemctl enable <init-file-name>`.
5. Start the init file using your system commands. For example `systemctl start <init-file-name>`.

You can manage the instance daemon through procServ, by telneting to the configured port. 

