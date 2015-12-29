#!/bin/sh
ssh w1-gst-dev29 iostat -k -d 2 | grep sda
