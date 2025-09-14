#!/usr/bin/python
#
# Copyright (C) 2025 Petr Bena <petr@bena.rocks>
#
# This program is free software; you can redistribute it and/or modify 
# it under the terms of the GNU Lesser General Public License as published 
# by the Free Software Foundation; version 2.1 only.
#
# This program is distributed in the hope that it will be useful, 
# but WITHOUT ANY WARRANTY; without even the implied warranty of 
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the 
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# LVMoRBDSR: LVHD over Ceph RBD storage repository
#

import SR, LVHDSR, SRCommand, util
import time
import os, sys
import xs_errors
import xmlrpclib
import json
import threading
import re
import lvutil
import scsiutil

CAPABILITIES = ["SR_PROBE", "SR_UPDATE", "SR_METADATA", "SR_TRIM",
                "VDI_CREATE", "VDI_DELETE", "VDI_ATTACH", "VDI_DETACH",
                "VDI_GENERATE_CONFIG", "VDI_CLONE", "VDI_SNAPSHOT",
                "VDI_RESIZE", "ATOMIC_PAUSE", "VDI_RESET_ON_BOOT/2",
                "VDI_UPDATE", "VDI_MIRROR", "VDI_CONFIG_CBT", 
                "VDI_ACTIVATE", "VDI_DEACTIVATE", "THIN_PROVISIONING"]

CONFIGURATION = [
    ['pool', 'Ceph pool name (required)'],
    ['rbd_image', 'RBD image name for shared LVM storage (required)'],
    ['conf', 'Path to ceph.conf file (optional, defaults to /etc/ceph/ceph.conf)'],
    ['user', 'Ceph username (optional, defaults to admin)'],
    ['keyring', 'Path to keyring file (optional)'],
    ['protected', 'If set to true, prevents deletion of the RBD image when the SR is deleted (defaults to true)'],
    ['mon_host', 'Comma-separated list of monitor hosts (optional)'],
    ['size', 'Size of the RBD image for SR creation (required for create). Supports units: 1024M, 10G, 2T (default: MB)']
]

DRIVER_INFO = {
    'name': 'LVHD over Ceph RBD',
    'description': 'SR plugin which represents disks as Logical Volumes within a Volume Group created on a Ceph RBD image',
    'vendor': 'Petr Bena <petr@bena.rocks>',
    'copyright': '(C) 2025 Petr Bena <petr@bena.rocks>',
    'driver_version': '1.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
}

class LVMoRBDSR(LVHDSR.LVHDSR):
    """LVHD over Ceph RBD storage repository"""

    LVMoRBD_CONF_DIR = '/etc/lvm/lvmorbd'
    
    def handles(type):
        return type == "lvmorbd"
    handles = staticmethod(handles)

    # Default XCP-ng lvm config doesn't support RBD images which is actually a good thing
    # because if VMs use RBD images directly (for example via RBDSR SM driver) and use
    # their own LVM by the guest OS, this LVM structure would be exposed to hypervisor
    # which would be confusing to admin, would slow all LVM calls by other LVM-based SRs
    # and would be generally dangerous. So we don't want to change that default behaviour.

    # For that reason we implement our own LVM config files in /etc/lvm/lvmorbd and use
    # these instead - they are configured to match only RBD devices and no other.
    def _with_rbd_lvm_conf(self, func, *args, **kwargs):
        """Execute function with RBD-aware LVM configuration"""
        # Save current LVM_SYSTEM_DIR
        old_lvm_dir = os.environ.get('LVM_SYSTEM_DIR', '')
        
        try:
            # Use the RBD-aware LVM configuration installed by the setup script
            if os.path.exists(self.LVMoRBD_CONF_DIR):
                util.SMlog("Using RBD-aware LVM configuration from %s" % self.LVMoRBD_CONF_DIR)
                os.environ['LVM_SYSTEM_DIR'] = self.LVMoRBD_CONF_DIR
            else:
                util.SMlog("Warning: RBD LVM configuration not found at %s, using default" % self.LVMoRBD_CONF_DIR)

            # Execute the function
            return func(*args, **kwargs)
        finally:
            # Restore original LVM_SYSTEM_DIR
            if old_lvm_dir:
                os.environ['LVM_SYSTEM_DIR'] = old_lvm_dir
            elif 'LVM_SYSTEM_DIR' in os.environ:
                del os.environ['LVM_SYSTEM_DIR']

    # Parent LVM SM class is sadly running scsi commands on every block device
    # RBD images don't support these calls, so we use this wrapper
    # to avoid misleading errors in logs
    def _create_lvm_on_rbd(self, sr_uuid):
        """Create LVM volume group on RBD device, skipping SCSI checks"""
        
        util.SMlog("Creating LVM VG on RBD device %s" % self.device_path)
        
        # Check if VG already exists
        if lvutil._checkVG(self.vgname):
            raise xs_errors.XenError('SRExists')

        # Skip SCSI serial number checks for RBD devices - they don't apply
        # Just check if device is already in use by other PBDs
        if util.test_hostPBD_devs(self.session, sr_uuid, self.device_path):
            raise xs_errors.XenError('SRInUse')

        # Create the VG on RBD device
        lvutil.createVG(self.device_path, self.vgname)
        
        # For RBD devices, we use a synthetic serial record based on pool/image name
        # This helps with device tracking without relying on SCSI serial numbers
        rbd_serial = "RBD:%s:%s" % (self.pool, self.rbd_image)
        scsiutil.add_serial_record(self.session, self.sr_ref, rbd_serial)
        
        # Enable VHD mode (required for LVHDSR functionality)
        self.session.xenapi.SR.add_to_sm_config(self.sr_ref, self.FLAG_USE_VHD, 'true')
        
        util.SMlog("Successfully created LVM VG %s on RBD device" % self.vgname)

    def _parse_size_with_units(self, size_str):
        """
        Parse a size string with optional units (M, G, T) and return size in MB.
        
        Args:
            size_str: Size string like "1024", "10G", "2T", "500M"
            
        Returns:
            int: Size in MB
            
        Examples:
            _parse_size_with_units("1024") -> 1024  (MB default)
            _parse_size_with_units("10G") -> 10240
            _parse_size_with_units("2T") -> 2097152
            _parse_size_with_units("500M") -> 500
        """
        if not size_str:
            raise ValueError("Size cannot be empty")
        
        # Remove any whitespace
        size_str = str(size_str).strip().upper()
        
        # Match number with optional unit
        match = re.match(r'^(\d+(?:\.\d+)?)\s*([KMGT]?)B?$', size_str)
        if not match:
            raise ValueError("Invalid size format. Use formats like: 1024, 10G, 2T, 500M")
        
        number = float(match.group(1))
        unit = match.group(2)
        
        # Convert to MB
        if unit == '' or unit == 'M':
            return int(number)
        elif unit == 'K':
            return int(number / 1024)
        elif unit == 'G':
            return int(number * 1024)
        elif unit == 'T':
            return int(number * 1024 * 1024)
        else:
            raise ValueError("Unsupported unit: %s" % unit)

    def load(self, sr_uuid):
        if not sr_uuid:
            # This is a probe call, generate a temp sr_uuid
            sr_uuid = util.gen_uuid()

        # Validate required configuration
        required_params = ['pool', 'rbd_image']
        missing_params = [param for param in required_params if param not in self.dconf]
        if missing_params:
            errstr = 'device-config is missing the following parameters: ' + ', '.join(missing_params)
            raise xs_errors.XenError('ConfigParamsMissing', opterr=errstr)

        # Store RBD configuration
        self.pool = self.dconf['pool']
        self.rbd_image = self.dconf['rbd_image']
        self.ceph_conf = self.dconf.get('conf', '/etc/ceph/ceph.conf')
        self.ceph_user = self.dconf.get('user', 'admin')
        self.keyring = self.dconf.get('keyring', '')
        self.mon_host = self.dconf.get('mon_host', '')
        self.protected = self.dconf.get('protected', 'true').lower() == 'true'
        
        # Build full RBD name
        self.rbd_name = "%s/%s" % (self.pool, self.rbd_image)
        
        # Initialize device path (will be set when mapped)
        self.device_path = None
        
        # Check if RBD is already mapped and set device path
        mapped_device = self._get_rbd_device_path()
        if mapped_device:
            self.device_path = mapped_device
            self.dconf['device'] = mapped_device
            self.device = mapped_device

        # Call parent load
        LVHDSR.LVHDSR.load(self, sr_uuid)

    def _build_rbd_cmd(self, args):
        """Build RBD command with proper authentication"""
        cmd = ['rbd']
        
        # Add configuration file
        if self.ceph_conf:
            cmd.extend(['-c', self.ceph_conf])
            
        # Add user authentication
        if self.ceph_user:
            cmd.extend(['--id', self.ceph_user])
            
        # Add keyring if specified
        if self.keyring:
            cmd.extend(['--keyring', self.keyring])
            
        # Add monitor hosts if specified
        if self.mon_host:
            cmd.extend(['-m', self.mon_host])
            
        # Add the actual command
        cmd.extend(args)
        
        return cmd

    def _get_rbd_device_path(self):
        """Get the current device path for our RBD image"""
        try:
            cmd = self._build_rbd_cmd(['showmapped', '--format', 'json'])
            output = util.pread2(cmd)
            
            if output.strip():
                mapped = json.loads(output)
                for info in mapped:
                    if (info['pool'] == self.pool and 
                        info['name'] == self.rbd_image):
                        return info['device']
        except Exception as e:
            util.SMlog("Failed to get RBD device path: %s" % str(e))
        
        return None
    
    def _check_if_rbd_mapped(self):
        """Check if the RBD image is currently mapped"""
        device_path = self._get_rbd_device_path()
        return device_path is not None

    def _map_rbd_image(self):
        """Map the RBD image to a block device"""
        util.SMlog("Mapping RBD image %s" % self.rbd_name)
        
        # Check if already mapped
        device_path = self._get_rbd_device_path()
        if device_path:
            util.SMlog("RBD image %s already mapped to %s" % (self.rbd_name, device_path))
            self.device_path = device_path
            return device_path
        
        try:
            # Map the RBD image
            cmd = self._build_rbd_cmd(['map', self.rbd_name])
            device_path = util.pread2(cmd).strip()
            
            if not device_path:
                raise Exception("RBD map returned empty device path")
            
            # Wait for device to be ready
            if not util.wait_for_path(device_path, 30):
                raise Exception("Device %s did not appear" % device_path)
            
            self.device_path = device_path
            util.SMlog("Successfully mapped RBD image %s to %s" % (self.rbd_name, device_path))
            return device_path
            
        except Exception as e:
            raise xs_errors.XenError('SRUnavailable', 
                                    opterr='Failed to map RBD image %s: %s' % (self.rbd_name, str(e)))

    def _unmap_rbd_image(self):
        """Unmap the RBD image"""
        if not self.device_path:
            self.device_path = self._get_rbd_device_path()
        
        if not self.device_path:
            util.SMlog("RBD image %s not mapped" % self.rbd_name)
            return
            
        util.SMlog("Unmapping RBD device %s" % self.device_path)
        
        try:
            cmd = self._build_rbd_cmd(['unmap', self.device_path])
            util.pread2(cmd)
            util.SMlog("Successfully unmapped RBD device %s" % self.device_path)
            self.device_path = None
        except Exception as e:
            util.SMlog("Warning: Failed to unmap RBD device %s: %s" % (self.device_path, str(e)))
            # Don't raise exception here as this might be called during cleanup

    def _ensure_rbd_mapped(self, sr_uuid):
        """Ensure the RBD image is mapped and ready"""
        self._map_rbd_image()
        
        # Set the path for LVHDSR
        if not hasattr(self, 'path'):
            self.path = self.device_path
        
        # Update dconf with the device path for LVM operations
        # This is critical for LVHDSR which expects self.dconf['device'] to exist
        self.dconf['device'] = self.device_path
        
        # Also ensure the device path is available as an attribute
        # for methods that might need direct access
        self.device = self.device_path

    def _create_rbd_image(self, size_mb):
        """Create the RBD image for the SR"""
        util.SMlog("Creating RBD image %s with size %d MB" % (self.rbd_name, size_mb))
        
        try:
            # Create RBD image with specified size and XCP-ng compatible features
            # Disable modern features that aren't supported by XCP-ng/Nautilus kernels
            cmd = self._build_rbd_cmd([
                'create', 
                '--size', str(size_mb),  # RBD uses MB
                '--image-format', '2',  # Use format 2 for advanced features
                '--image-feature', 'layering',  # Only enable basic layering feature
                self.rbd_name
            ])
            util.pread2(cmd)
            
            util.SMlog("Successfully created RBD image %s" % self.rbd_name)
            
        except Exception as e:
            raise xs_errors.XenError('SRUnavailable',
                                   opterr='Failed to create RBD image %s: %s' % (self.rbd_name, str(e)))

    def _delete_rbd_image(self):
        """Delete the RBD image"""

        if self.protected:
            util.SMlog("RBD image %s is protected, skipping deletion" % self.rbd_name)
            return

        util.SMlog("Deleting RBD image %s" % self.rbd_name)
        
        try:
            cmd = self._build_rbd_cmd(['rm', self.rbd_image, '--pool', self.pool])
            util.pread2(cmd)
            util.SMlog("Successfully deleted RBD image %s" % self.rbd_name)
        except Exception as e:
            raise xs_errors.XenError('SRUnavailable',
                                   opterr='Failed to delete RBD image %s: %s' % (self.rbd_name, str(e)))

    def create(self, sr_uuid, size):
        """Create the SR - create RBD image and LVM volume group
        
        Args:
            sr_uuid: Storage Repository UUID
            size: SR size in bytes from XCP-ng (usually ignored for LVM SRs)
                 The actual SR size will be determined by the RBD image size
        """
        util.SMlog("Creating LVMoRBD SR %s (XCP-ng requested size: %s bytes)" % (sr_uuid, size))
        
        # Get RBD image size from configuration with unit parsing
        if 'size' not in self.dconf:
            errstr = 'device-config is missing the following parameters: size (RBD image size, e.g., 10G, 2T, 1024M)'
            raise xs_errors.XenError('ConfigParamsMissing', opterr=errstr)
        
        try:
            rbd_size_mb = self._parse_size_with_units(self.dconf['size'])
            util.SMlog("Parsed RBD image size: %d MB from input '%s'" % (rbd_size_mb, self.dconf['size']))
        except ValueError as e:
            raise xs_errors.XenError('ConfigDeviceInvalid', 
                                   opterr='Invalid size format: %s. Use formats like: 1024, 10G, 2T, 500M' % str(e))
        
        # Create the RBD image
        self._create_rbd_image(rbd_size_mb)
        
        if ('protected' in self.dconf and self.dconf['protected'].lower() == 'false'):
            self.protected = False
        
        try:
            # Map the RBD image
            self._ensure_rbd_mapped(sr_uuid)
            
            # Create LVM volume group on the RBD device, skipping SCSI checks
            # Use our custom method that doesn't assume SCSI devices
            self._with_rbd_lvm_conf(self._create_lvm_on_rbd, sr_uuid)
            
        except Exception as e:
            # Clean up RBD image if LVM creation failed
            try:
                self._unmap_rbd_image()
                self._delete_rbd_image()
            except:
                pass
            raise

    def _deactivate_vg(self):
        """Deactivate all logical volumes in the volume group"""
        util.SMlog("Deactivating VG: %s" % self.vgname)
        try:
            # Use vgchange to deactivate all LVs in the VG
            cmd = ['/sbin/vgchange', '-a', 'n', self.vgname]
            util.pread2(cmd)
            util.SMlog("Successfully deactivated VG: %s" % self.vgname)
        except Exception as e:
            util.SMlog("Warning: Failed to deactivate VG %s: %s" % (self.vgname, str(e)))
            raise

    def _export_vg(self):
        """Export the volume group to disconnect it from LVM"""
        util.SMlog("Exporting VG: %s" % self.vgname)
        try:
            # Use vgexport to cleanly disconnect the VG
            cmd = ['/sbin/vgexport', self.vgname]
            util.pread2(cmd)
            util.SMlog("Successfully exported VG: %s" % self.vgname)
        except Exception as e:
            util.SMlog("Warning: Failed to export VG %s: %s" % (self.vgname, str(e)))
            raise

    def delete(self, sr_uuid):
        """Delete the SR - remove LVM volume group and RBD image"""
        util.SMlog("Deleting LVMoRBD SR %s" % sr_uuid)

        if self.protected:
            # For protected images, deactivate and export VG before unmapping RBD
            if (self._check_if_rbd_mapped()):
                try:
                    util.SMlog("Deactivating and exporting VG for protected RBD image %s" % self.rbd_name)
                    
                    # Deactivate all LVs in the VG to release locks
                    self._with_rbd_lvm_conf(self._deactivate_vg)
                    
                    # Export the VG to cleanly disconnect from LVM
                    self._with_rbd_lvm_conf(self._export_vg)
                    
                    util.SMlog("Successfully deactivated and exported VG for %s" % self.vgname)
                    
                except Exception as e:
                    util.SMlog("Warning: Failed to cleanly deactivate VG: %s" % str(e))
                    # Continue anyway to attempt RBD unmap
                
                # Now unmap the RBD image
                self._unmap_rbd_image()

            util.SMlog("RBD image %s is protected, skipping deletion" % self.rbd_name)
            return
        
        if (self._check_if_rbd_mapped()):
            # Delete LVM volume group with RBD-aware configuration
            # Actually here we could probably just export it, which would be faster
            # we are about to wipe the RBD anyway
            self._with_rbd_lvm_conf(LVHDSR.LVHDSR.delete, self, sr_uuid)
            self._unmap_rbd_image()
        
        self._delete_rbd_image()

    def attach(self, sr_uuid):
        """Attach the SR - map RBD image and activate LVM"""
        util.SMlog("Attaching LVMoRBD SR %s" % sr_uuid)
        
        try:
            # Map the RBD image
            self._ensure_rbd_mapped(sr_uuid)
            
            # Attach LVM volume group with RBD-aware configuration
            self._with_rbd_lvm_conf(LVHDSR.LVHDSR.attach, self, sr_uuid)
            
        except Exception as e:
            # Clean up on failure
            self._unmap_rbd_image()
            raise xs_errors.XenError("SRUnavailable", opterr=str(e))

    def detach(self, sr_uuid):
        """Detach the SR - deactivate LVM and unmap RBD image"""
        util.SMlog("Detaching LVMoRBD SR %s" % sr_uuid)
        
        try:
            # Detach LVM volume group first with RBD-aware configuration
            self._with_rbd_lvm_conf(LVHDSR.LVHDSR.detach, self, sr_uuid)
        finally:
            # Always try to unmap RBD image
            util.SMlog("Unmapping RBD image during detach")
            self._unmap_rbd_image()

    def _refresh_size(self):
        """Override LVHDSR._refresh_size to handle RBD device properly"""
        # Ensure we have the device path set
        if not self.device_path:
            self.device_path = self._get_rbd_device_path()
        
        if not self.device_path:
            util.SMlog("Warning: RBD device not mapped, cannot refresh size")
            return False
        
        # Ensure dconf has the device path
        self.dconf['device'] = self.device_path
        
        # Call parent _refresh_size with RBD-aware LVM config
        return self._with_rbd_lvm_conf(LVHDSR.LVHDSR._refresh_size, self)

    def scan(self, sr_uuid):
        """Scan the SR - scan LVM with RBD-aware configuration"""
        # Ensure RBD is mapped and device path is set
        if not self.device_path:
            mapped_device = self._get_rbd_device_path()
            if mapped_device:
                self.device_path = mapped_device
                self.dconf['device'] = mapped_device
                self.device = mapped_device
        
        self._with_rbd_lvm_conf(LVHDSR.LVHDSR.scan, self, sr_uuid)

    def probe(self):
        """Probe for existing SR"""
        self.uuid = util.gen_uuid()
        
        try:
            # RBD may already be mapped, since it's a PV we shouldn't map it twice, but also in case
            # it's already mapped we shouldn't unmap it later
            is_mapped = self._check_if_rbd_mapped()

            if is_mapped is False:
                # Map RBD image to check if it exists and has LVM
                self._ensure_rbd_mapped(self.uuid)
            
            # Probe LVM on the device with RBD-aware configuration
            out = self._with_rbd_lvm_conf(LVHDSR.LVHDSR.probe, self)
            
            return out
            
        except Exception as e:
            raise xs_errors.XenError('SRUnavailable', opterr=str(e))
        finally:
            if is_mapped is False:
                self._unmap_rbd_image()

    def vdi(self, uuid):
        return LVMoRBDVDI(self, uuid)

class LVMoRBDVDI(LVHDSR.LVHDVDI):
    """VDI class for LVMoRBD SR"""
    
    def generate_config(self, sr_uuid, vdi_uuid):
        """Generate configuration for VDI attach from config"""
        util.SMlog("LVMoRBDVDI.generate_config")
        
        if not self.sr.device_path:
            raise xs_errors.XenError('VDIUnavailable', 
                                   opterr='RBD device not mapped')
        
        dict = {}
        dict['device_config'] = self.sr.dconf.copy()
        dict['sr_uuid'] = sr_uuid
        dict['vdi_uuid'] = vdi_uuid
        dict['command'] = 'vdi_attach_from_config'
        
        # Return the 'config' encoded within a normal XMLRPC response
        config = xmlrpclib.dumps(tuple([dict]), "vdi_attach_from_config")
        return xmlrpclib.dumps((config,), "", True)

    def attach_from_config(self, sr_uuid, vdi_uuid):
        """Attach VDI from configuration (used for suspend/resume)"""
        util.SMlog("LVMoRBDVDI.attach_from_config")
        
        try:
            # Attach the LVM logical volume
            return LVHDSR.LVHDVDI.attach(self, sr_uuid, vdi_uuid)
            
        except Exception as e:
            util.logException("LVMoRBDVDI.attach_from_config")
            raise xs_errors.XenError('SRUnavailable',
                                   opterr='Unable to attach VDI: %s' % str(e))

if __name__ == '__main__':
    SRCommand.run(LVMoRBDSR, DRIVER_INFO)
else:
    SR.registerSR(LVMoRBDSR)