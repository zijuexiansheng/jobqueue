# jobqueue
Issuing commands earlier, while executing them one another. You don't have to wait for one job to finish and then type the next command to execute

The reason of implementing this jobqueue is that some jobs are using lots of resources. If many such jobs are running at the same time, the overhead for the machine will be very large. As a result, executing each commands in sequence is a demand. But the jobs will be running for a long time. And we don't want to wait for one to finish and then type the next command. If you write a bash script, then everything should be fine. But if you forgot, or don't want to. Then this is what you are looking for.

If you want parallel jobs with dependencies, Makefile would be a much better choice.

# Dependencies

* python2

# Install

* `./install.sh`

# Usage

* `jobqueue -h`

