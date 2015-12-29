--------------------------------------------------------------------------------
commands_for_secure_hdfs.sh

This script must live in /usr/localnew/scripts. It must be owned by root for
security reasons, and have sudo privileges.

Support for different branches and builds, if needed, must be baked into the
script, since sudo requires a static path and a script for each build would
make the sudoers file unwieldy.
--------------------------------------------------------------------------------
