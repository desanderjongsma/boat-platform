#!/bin/bash
# Run once on the Raspberry Pi after flashing

set -e

echo "=== Edge Agent Setup ==="

# Enable SPI and CAN overlay for PiCAN-M
if ! grep -q "mcp2515" /boot/config.txt; then
    echo "dtoverlay=mcp2515-can0,oscillator=16000000,interrupt=25" >> /boot/config.txt
    echo "dtoverlay=spi-bcm2835-overlay" >> /boot/config.txt
    echo "CAN overlay added — reboot required"
fi

# Install dependencies
apt update
apt install -y python3-pip can-utils

pip3 install python-can paho-mqtt pyyaml --break-system-packages

# Create directories
mkdir -p /opt/edge-agent/data
mkdir -p /opt/edge-agent/config
mkdir -p /opt/edge-agent/logs

# Copy files
cp -r /home/pi/boat-platform/edge-agent/src /opt/edge-agent/
cp /home/pi/boat-platform/edge-agent/config/config.yaml /opt/edge-agent/config/

echo "=== Done — reboot the Pi ==="
