friendly tips:
1. try the keyboard “return” for prompt command will give you more space.
2. tip 1 is always a good idea to check if the current process is finished. Press the keyboard “return”, and prompt “linda> ” will show out after the current process is done.
3. P2 is white space free. So don’t worry about the white space you entered. But please don’t enter white space at the beginning of a command (inside the command is ok).

Q & A:
1. If user out a tuple “3,4,5”, the out print message will show “544->3,4,5”. What is 544?
The integer showing ahead “->” means the slot number this tuple saved at. It just used for tracking tuples. The content of tuple is showing after “->”.

2. Can user see the host ID?
No, host id is saved locally inside the program. The user can distinguish different host with their host name. Host ID will not be reported from out print.

2. How if user killed a host while this hosts is waiting for “in” or “rd” command of upcoming tuple?
while a host is waiting for a tuple to be “in”, this host gets killed. Then we recognize this situation as the command is not executed successfully. So during the time when this host is down, if this tuple gets added. This tuple will not be deleted. The user need to do “in” this tuple again, after the crashed host back online.


