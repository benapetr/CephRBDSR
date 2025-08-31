#!/bin/bash
# install_cephrbd.sh - Installation script for CephRBDSR driver

set -e

echo "Installing CephRBDSR driver for XCP-ng..."

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

# Check for required dependencies
echo "Checking dependencies..."

# Check for Ceph RBD tools
if ! command -v rbd &> /dev/null; then
    echo "Error: Ceph RBD tools not found. Please install ceph-common package:"
    echo "  yum install ceph-common"
    exit 1
fi

if ! command -v ceph &> /dev/null; then
    echo "Error: Ceph CLI tools not found. Please install ceph-common package:"
    echo "  yum install ceph-common"
    exit 1
fi

echo "✓ Ceph tools found"

# Install the driver
echo "Installing CephRBDSR driver..."

# Copy driver file to SM directory
cp CephRBDSR.py /opt/xensource/sm/
chmod +x /opt/xensource/sm/CephRBDSR.py

# Create executable entry point (this is crucial for SMAPIv2)
cp CephRBDSR.py /opt/xensource/sm/CephRBDSR
chmod +x /opt/xensource/sm/CephRBDSR

echo "✓ Driver files installed"

# Verify installation
echo "Verifying installation..."

# Test driver import and execution pattern
python -c "
import sys
sys.path.insert(0, '/opt/xensource/sm')
import CephRBDSR
print('✓ Driver imports successfully')
print('✓ Driver handles type:', CephRBDSR.CephRBDSR.handles('cephrbd'))
print('✓ Driver has DRIVER_INFO:', hasattr(CephRBDSR, 'DRIVER_INFO'))
"

# Test that the executable entry point exists
if [ ! -x "/opt/xensource/sm/CephRBDSR" ]; then
    echo "✗ Error: CephRBDSR entry point is not executable"
    exit 1
fi

echo "✓ All installation checks passed"

echo ""
echo "CephRBDSR driver installed successfully!"

echo ""
echo "The driver is now ready to use. XCP-ng will automatically discover it."
echo "You can now create Ceph RBD storage repositories using:"

echo ""
echo "Prerequisites:"
echo "1. Ensure Ceph cluster is running and accessible"
echo "2. Configure /etc/ceph/ceph.conf with cluster details"
echo "3. Set up authentication keyring if using cephx"
echo "4. Create a Ceph pool for XCP-ng storage"
echo ""
echo "Usage example:"
echo "  # Create Ceph pool (run on Ceph cluster):"
echo "  ceph osd pool create xcpng 128 128"
echo "  ceph osd pool application enable xcpng rbd"
echo ""
echo "  # Create SR in XCP-ng:"
echo "  xe sr-create name-label='Ceph RBD Storage' type=cephrbd \\"
echo "    device-config:pool=xcpng \\"
echo "    device-config:ceph_user=admin"
echo ""
echo "Optional configuration parameters:"
echo "  device-config:ceph_conf=/path/to/ceph.conf"
echo "  device-config:keyring=/path/to/keyring"
echo "  device-config:mon_host=mon1,mon2,mon3"
