#! /bin/bash
#set -e

# Go to the .ssh directory
if [ ! -d ~/.ssh ]; then
  mkdir ~/.ssh
fi
cd ~/.ssh

if [ ! -f id_dsa ]; then
  echo "Creating id_dsa"
  # Create the identity
  ssh-keygen -q -b 1024 -t dsa -f id_dsa -N ""
  if [ $? -ne 0 ]; then
    echo "ssh-keygen failure"
   exit 1
  fi
else
  echo "  Note: id_dsa already exists"
fi

k=`cat id_dsa.pub`
missing=1
if [ -f authorized_keys ]; then
  missing=`grep -q "$k" authorized_keys`
else
  echo "  Note: no authorized_keys file (yet)"
fi
if [ $missing ]; then
  echo "  Adding id_dsa.pub to authorized_keys..."
  cat id_dsa.pub >> authorized_keys
else
  echo "  Note: id_dsa.pub already in authorized_keys"
fi

missing=1
if [ -f config ]; then
  missing=`grep -q StrictHostKeyChecking config`
else
  echo "  Note: no config file (yet)"
fi
if [ $missing ]; then
  echo "  Disabling StrictHostKeyChecking in config..."
  echo "StrictHostKeyChecking no" >> config
else
  echo "  Note: StrictHostKeyChecking option already set."
fi

echo "  Making all of these .ssh files private..."
chmod 600 id_dsa.pub authorized_keys config
if [ $? -ne 0 ]; then
  echo "chmod failure"
  exit 1
fi

cd ..
echo "  Zipping your .ssh folder..."
zip -qr ssh.zip .ssh
if [ $? -ne 0 ]; then
  echo "Unable to zip up your .ssh folder"
  exit 1
fi

echo "  Moving ssh.zip to ~/.ssh..."
mv ssh.zip .ssh
if [ $? -ne 0 ]; then
  echo "Unable to move your ssh.zip"
fi

echo "  Making your ssh.zip private..."
chmod 600 .ssh/ssh.zip
if [ $? -ne 0 ]; then
  echo "Unable to chmod  your .ssh/ssh.zip"
  exit 1
fi

echo "Congratulations!"
echo "For proper operation, unpack your ssh.zip on remote machines."
exit 0
