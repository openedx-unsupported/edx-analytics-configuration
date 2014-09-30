#!/bin/bash

sudo sh -c 'echo "deb http://ftp.us.debian.org/debian squeeze-lts main" >>/etc/apt/sources.list'
sudo apt-get update
sudo apt-get install -y bash=4.1-3+deb6u2
