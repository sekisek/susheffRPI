#!/bin/bash
sudo systemctl stop instagram-monitor.service
cd /home/bamanio/social-bot/app || exit 1
source /home/bamanio/social-bot/.venv/bin/activate
python relogin_instagram.py
sudo systemctl start instagram-monitor.service
sudo systemctl status instagram-monitor.service

