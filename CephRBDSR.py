#!/usr/bin/python
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
"""
CephRBDSR.py - Ceph RBD Storage Repository Driver
Provides integration with Ceph RADOS Block Device storage for XCP-ng

This driver allows XCP-ng to use Ceph RBD images as virtual disk storage,
providing distributed, scalable block storage with features like:
- Thin provisioning
- Snapshots and clones 
- High availability through replication
- Live migration support
"""

import SR
import VDI
import SRCommand
import util
import xs_errors
import vhdutil
import os
import json
import time
import subprocess
import re
import xmlrpclib
from lock import Lock

# Driver capabilities - Ceph RBD supports advanced features
CAPABILITIES = [
    "SR_PROBE",              # Can probe for existing pools
    "SR_UPDATE",             # Can update repository properties
    "VDI_CREATE",            # Can create new RBD images
    "VDI_DELETE",            # Can delete RBD images
    "VDI_ATTACH",            # Can map RBD images as block devices
    "VDI_DETACH",            # Can unmap RBD images
    "VDI_CLONE",             # Can clone RBD images (COW)
    "VDI_SNAPSHOT",          # Can create RBD snapshots
    "VDI_RESIZE",            # Can resize RBD images
    "VDI_RESIZE_ONLINE",     # Can resize while VM is running
    "THIN_PROVISIONING",     # RBD supports thin provisioning
    "VDI_READ_CACHING",      # Supports read caching
    "VDI_CONFIG_CBT",        # Supports changed block tracking via snapshots
]

# Configuration parameters required for Ceph RBD
CONFIGURATION = [
    ['pool', 'Ceph pool name (required)'],
    ['conf', 'Ceph configuration file path (optional, default /etc/ceph/ceph.conf)'],
    ['user', 'Ceph user name (optional, default admin)']
]

# Driver metadata information
DRIVER_INFO = {
    'name': 'Ceph RBD Storage',
    'description': 'Ceph RADOS Block Device storage repository for distributed storage',
    'vendor': 'Petr Bena <petr@bena.rocks>',
    'copyright': '(C) 2025 Petr Bena <petr@bena.rocks>',
    'driver_version': '1.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
}

# Driver-specific configuration
DRIVER_CONFIG = {
    "ATTACH_FROM_CONFIG_WITH_TAPDISK": False,  # We use kernel RBD mapping
    "MULTIPATH": False  # Ceph handles replication internally
}

class CephRBDSR(SR.SR):
    """Ceph RBD Storage Repository implementation"""
    
    DRIVER_TYPE = "cephrbd"
    RBD_PREFIX = "vdi-"  # Prefix for RBD image names
    
    def handles(type):
        """Check if this driver handles the given storage type"""
        return type == CephRBDSR.DRIVER_TYPE
    handles = staticmethod(handles)
    
    def load(self, sr_uuid):
        """Load and validate SR configuration"""
        util.SMlog("Loading CephRBDSR %s" % sr_uuid)
        
        # Validate required configuration
        if 'pool' not in self.dconf:
            raise xs_errors.XenError('ConfigParameterMissing',
                                   opterr='Missing required parameter: pool')
        
        # Store configuration
        self.pool = self.dconf['pool']
        self.ceph_conf = self.dconf.get('ceph_conf', '/etc/ceph/ceph.conf')
        self.ceph_user = self.dconf.get('ceph_user', 'admin')
        self.keyring = self.dconf.get('keyring', '')
        self.mon_host = self.dconf.get('mon_host', '')
        
        # Validate Ceph configuration file exists
        if not os.path.exists(self.ceph_conf):
            raise xs_errors.XenError('ConfigParameterInvalid',
                                   opterr='Ceph config file not found: %s' % self.ceph_conf)
        
        # Set up paths and properties
        self.path = "/dev/rbd/%s" % self.pool  # Kernel RBD device path
        self.driver_config = DRIVER_CONFIG
        
        # Initialize locking
        self.lock = Lock("CephRBDSR", sr_uuid)
        
        # Test Ceph connectivity
        self._test_ceph_connectivity()
        
        util.SMlog("CephRBDSR loaded successfully: pool=%s" % self.pool)
    
    def create(self, sr_uuid, size):
        """Create new Ceph RBD storage repository"""
        util.SMlog("Creating CephRBDSR %s with size %d in pool %s" % 
                   (sr_uuid, size, self.pool))
        
        # Verify pool exists and is accessible
        try:
            cmd = self._build_ceph_cmd(['osd', 'pool', 'stats', self.pool])
            util.pread2(cmd)
            util.SMlog("Verified pool %s exists" % self.pool)
        except Exception as e:
            raise xs_errors.XenError('SRUnavailable',
                                   opterr='Cannot access Ceph pool %s: %s' % (self.pool, str(e)))
        
        # Initialize statistics - virtual allocation starts at 0
        self.virtual_allocation = 0
        
        # Update physical statistics from pool
        self.stat(sr_uuid)
        
        util.SMlog("CephRBDSR created successfully")
    
    def delete(self, sr_uuid):
        """Delete Ceph RBD storage repository"""
        util.SMlog("Deleting CephRBDSR %s" % sr_uuid)
        
        # First scan for any existing VDIs
        self.scan(sr_uuid)
        
        # Delete all VDIs in this SR
        for vdi_uuid in list(self.vdis.keys()):
            vdi = self.vdis[vdi_uuid]
            try:
                vdi.delete(sr_uuid, vdi_uuid)
                util.SMlog("Deleted VDI %s" % vdi_uuid)
            except Exception as e:
                util.SMlog("Warning: Failed to delete VDI %s: %s" % (vdi_uuid, str(e)))
        
        # Note: We don't delete the Ceph pool as it may contain other data
        # and pools are typically managed separately from XCP-ng
        
        util.SMlog("CephRBDSR deleted successfully")
    
    def attach(self, sr_uuid):
        """Attach Ceph RBD storage repository"""
        util.SMlog("Attaching CephRBDSR %s" % sr_uuid)
        
        # Verify Ceph connectivity
        self._test_ceph_connectivity()
        
        # Update pool statistics
        self.stat(sr_uuid)
        
        # Scan for existing VDIs
        self.scan(sr_uuid)
        
        util.SMlog("CephRBDSR attached successfully")
    
    def detach(self, sr_uuid):
        """Detach Ceph RBD storage repository"""
        util.SMlog("Detaching CephRBDSR %s" % sr_uuid)
        
        # Ensure all VDIs are detached
        for vdi_uuid in self.vdis:
            vdi = self.vdis[vdi_uuid]
            if vdi.attached:
                util.SMlog("Force detaching VDI %s" % vdi_uuid)
                try:
                    vdi.detach(sr_uuid, vdi_uuid)
                except Exception as e:
                    util.SMlog("Warning: Failed to detach VDI %s: %s" % (vdi_uuid, str(e)))
        
        util.SMlog("CephRBDSR detached successfully")
    
    def scan(self, sr_uuid):
        """Scan Ceph pool for RBD images belonging to this SR"""
        util.SMlog("Scanning CephRBDSR %s for VDIs" % sr_uuid)
        
        self.vdis = {}
        self.virtual_allocation = 0
        
        try:
            # List all RBD images in the pool
            cmd = self._build_rbd_cmd(['ls', '-l', '--format', 'json'])
            output = util.pread2(cmd)
            
            if output.strip():
                rbd_images = json.loads(output)
                
                for image_info in rbd_images:
                    image_name = image_info['image']
                    
                    # Check if this RBD image belongs to our SR (has VDI prefix and proper UUID)
                    if image_name.startswith(self.RBD_PREFIX):
                        vdi_uuid = image_name[len(self.RBD_PREFIX):]
                        
                        # Validate UUID format
                        if self._is_valid_uuid(vdi_uuid):
                            try:
                                vdi = self.vdi(vdi_uuid)
                                vdi._load_from_rbd_info(image_info)
                                self.vdis[vdi_uuid] = vdi
                                self.virtual_allocation += vdi.size
                                util.SMlog("Found VDI: %s (size: %d)" % (vdi_uuid, vdi.size))
                            except Exception as e:
                                util.SMlog("Error loading VDI %s: %s" % (vdi_uuid, str(e)))
                
                util.SMlog("Scan complete: found %d VDIs, total allocation: %d" % 
                          (len(self.vdis), self.virtual_allocation))
            
        except Exception as e:
            util.SMlog("Error during scan: %s" % str(e))
            raise xs_errors.XenError('SRScanError', opterr=str(e))
    
    def vdi(self, uuid):
        """Create VDI object for given UUID"""
        return CephRBDVDI(self, uuid)

    def stat(self, sr_uuid):
        """Update SR capacity/usage statistics and sync with XAPI."""
        util.SMlog("CephRBDSR.stat: updating statistics for SR %s" % sr_uuid)
        
        s = self._get_pool_stats()
        total = s.get('total', 0)
        used = s.get('used', 0)

        # Guard: avoid total==0 (XAPI will display 1 byte)
        if total <= 0:
            util.SMlog("CephRBDSR.stat: total==0; using clamp value")
            total = 1
            used = 0

        # Always update physical statistics from Ceph
        self.physical_size = long(total)
        self.physical_utilisation = long(used)

        # Preserve existing virtual allocation if it's valid, otherwise get from XAPI
        if not hasattr(self, 'virtual_allocation') or self.virtual_allocation is None:
            self.virtual_allocation = 0
            if self.sr_ref:
                try:
                    valloc = int(self.session.xenapi.SR.get_virtual_allocation(self.sr_ref))
                    self.virtual_allocation = valloc
                    util.SMlog("CephRBDSR.stat: got virtual_allocation from XAPI: %d" % valloc)
                except Exception as e:
                    util.SMlog("CephRBDSR.stat: failed to get virtual_allocation from XAPI: %s" % str(e))

        free = max(0, self.physical_size - self.physical_utilisation)
        util.SMlog("CephRBDSR.stat: phys_size=%d phys_util=%d virt_alloc=%d free=%d" % 
                   (self.physical_size, self.physical_utilisation, self.virtual_allocation, free))

        # Update XAPI database with new statistics
        self._db_update()

    def update(self, sr_uuid):
        """Update SR statistics (same as stat for CephRBD)."""
        util.SMlog("CephRBDSR.update: updating SR %s" % sr_uuid)
        self.stat(sr_uuid)

    def _updateStats(self, sr_uuid, virtAllocDelta=0):
        """Update SR statistics with virtual allocation delta"""
        util.SMlog("CephRBDSR._updateStats: delta=%d" % virtAllocDelta)
        
        # Update virtual allocation
        if virtAllocDelta != 0:
            if self.sr_ref:
                try:
                    # Get current virtual allocation from XAPI and add delta
                    valloc = int(self.session.xenapi.SR.get_virtual_allocation(self.sr_ref))
                    self.virtual_allocation = valloc + virtAllocDelta
                    util.SMlog("CephRBDSR._updateStats: XAPI virtual_allocation %d + delta %d = %d" % 
                               (valloc, virtAllocDelta, self.virtual_allocation))
                except Exception as e:
                    util.SMlog("Warning: Could not get virtual allocation from XAPI: %s" % str(e))
                    # Fall back to our own tracking
                    if not hasattr(self, 'virtual_allocation'):
                        self.virtual_allocation = 0
                    self.virtual_allocation += virtAllocDelta
                    util.SMlog("CephRBDSR._updateStats: local virtual_allocation + delta %d = %d" % 
                               (virtAllocDelta, self.virtual_allocation))
        
        # Always refresh physical statistics from pool
        s = self._get_pool_stats()
        total = s.get('total', 0)
        used = s.get('used', 0)
        
        if total <= 0:
            total = 1
            used = 0
            
        self.physical_size = long(total)
        self.physical_utilisation = long(used)
        
        # Ensure we have virtual allocation set
        if not hasattr(self, 'virtual_allocation'):
            self.virtual_allocation = 0
        
        util.SMlog("CephRBDSR._updateStats: final stats phys_size=%d phys_util=%d virt_alloc=%d" % 
                   (self.physical_size, self.physical_utilisation, self.virtual_allocation))
        
        # Update XAPI database
        self._db_update()
    
    def _test_ceph_connectivity(self):
        """Test connectivity to Ceph cluster"""
        try:
            # Test basic Ceph connectivity
            cmd = self._build_ceph_cmd(['health'])
            output = util.pread2(cmd)
            util.SMlog("Ceph cluster health: %s" % output.strip())
            
            # Test pool access
            cmd = self._build_rbd_cmd(['ls', self.pool])
            util.pread2(cmd)
            util.SMlog("Successfully connected to Ceph pool: %s" % self.pool)
            
        except Exception as e:
            raise xs_errors.XenError('SRUnavailable',
                                   opterr='Cannot connect to Ceph cluster: %s' % str(e))
    
    def _get_pool_quota(self):
        """Check if pool has quota set and return quota-based statistics."""
        try:
            cmd = self._build_ceph_cmd(['osd', 'pool', 'get-quota', self.pool, '--format', 'json'])
            out = util.pread2(cmd)
            quota_data = json.loads(out)
            
            # Check if byte quota is set (quota_max_bytes > 0)
            quota_max_bytes = int(quota_data.get('quota_max_bytes', 0))
            if quota_max_bytes <= 0:
                util.SMlog("CephRBDSR: no byte quota set for pool %s" % self.pool)
                return None
            
            # Get current usage
            current_bytes = int(quota_data.get('current_num_bytes', 0))
            
            # Calculate free space within quota
            free_bytes = max(0, quota_max_bytes - current_bytes)
            
            util.SMlog("CephRBDSR: pool quota found - max: %d, used: %d, free: %d" % 
                      (quota_max_bytes, current_bytes, free_bytes))
            
            return {
                'total': quota_max_bytes,
                'used': current_bytes, 
                'free': free_bytes
            }
            
        except Exception as e:
            util.SMlog("CephRBDSR: failed to get pool quota: %s" % str(e))
            return None

    def _get_pool_stats(self):
        """Get statistics for the Ceph pool (bytes)."""
        try:
            # First check if the pool has a quota set
            quota_stats = self._get_pool_quota()
            if quota_stats:
                util.SMlog("CephRBDSR: using pool quota: total=%d used=%d" % 
                          (quota_stats['total'], quota_stats['used']))
                return quota_stats
            
            # Fall back to cluster capacity if no quota is set
            util.SMlog("CephRBDSR: no pool quota set, using cluster capacity")
            cmd = self._build_ceph_cmd(['df', 'detail', '-f', 'json'])
            out = util.pread2(cmd)
            data = json.loads(out)

            for p in data.get('pools', []):
                if p.get('name') == self.pool:
                    stats = p.get('stats', {}) or {}
                    # Ceph outputs:
                    #  - max_avail    : free bytes (per-pool, policy-aware)
                    #  - stored       : logical bytes stored (user-visible)
                    #  - bytes_used   : raw bytes (includes replication) - optional per pool
                    free  = int(stats.get('max_avail', 0) or 0)
                    used  = int((stats.get('stored')
                                if 'stored' in stats else stats.get('bytes_used', 0)) or 0)
                    total = free + used
                    return {'free': free, 'used': used, 'total': total}

            raise Exception("Pool %s not found in ceph df" % self.pool)
        except Exception as e:
            util.SMlog("CephRBDSR: df parse failed: %s" % str(e))
            return {'free': 0, 'used': 0, 'total': 0}
    
    def _build_ceph_cmd(self, args):
        """Build ceph command with authentication parameters"""
        cmd = ['ceph']
        
        # Add configuration file
        if self.ceph_conf:
            cmd.extend(['-c', self.ceph_conf])
        
        # Add user authentication
        if self.ceph_user:
            cmd.extend(['--id', self.ceph_user])
        
        # Add keyring if specified
        if self.keyring:
            cmd.extend(['-k', self.keyring])
        
        # Add monitor hosts if specified
        if self.mon_host:
            cmd.extend(['-m', self.mon_host])
        
        cmd.extend(args)
        return cmd
    
    def _build_rbd_cmd(self, args):
        """Build rbd command with authentication parameters"""
        cmd = ['rbd']
        
        # Add pool specification
        if self.pool and len(args) > 0 and not any('--pool' in str(arg) for arg in args):
            cmd.extend(['--pool', self.pool])
        
        # Add configuration file
        if self.ceph_conf:
            cmd.extend(['-c', self.ceph_conf])
        
        # Add user authentication  
        if self.ceph_user:
            cmd.extend(['--id', self.ceph_user])
        
        # Add keyring if specified
        if self.keyring:
            cmd.extend(['-k', self.keyring])
        
        # Add monitor hosts if specified
        if self.mon_host:
            cmd.extend(['-m', self.mon_host])
        
        cmd.extend(args)
        return cmd
    
    def _is_valid_uuid(self, uuid_str):
        """Validate UUID format"""
        uuid_pattern = re.compile(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            re.IGNORECASE
        )
        return bool(uuid_pattern.match(uuid_str))


class CephRBDVDI(VDI.VDI):
    """Ceph RBD VDI implementation"""
    
    def __init__(self, sr, uuid):
        VDI.VDI.__init__(self, sr, uuid)
        
        # RBD-specific properties
        self.rbd_name = "%s%s" % (sr.RBD_PREFIX, uuid)
        self.device_path = "/dev/rbd/%s/%s" % (sr.pool, self.rbd_name)
        self.mapped = False
        
        # Set VDI type - RBD provides raw block devices
        self.vdi_type = vhdutil.VDI_TYPE_RAW  # 'aio'
    
    def create(self, sr_uuid, vdi_uuid, size):
        """Create new RBD image"""
        util.SMlog("Creating CephRBD VDI %s, size %d" % (vdi_uuid, size))
        
        # Set VDI properties
        self.uuid = vdi_uuid
        self.location = vdi_uuid  # For RBD, location is the VDI UUID
        self.size = long(size)
        self.utilisation = 0  # Ceph uses thin provisioning
        self.vdi_type = vhdutil.VDI_TYPE_RAW  # RBD provides raw block devices
        
        # Check available space in pool
        pool_stats = self.sr._get_pool_stats()
        available = pool_stats.get('total', 0) - pool_stats.get('used', 0)
        
        if available > 0 and size > available:
            quota_stats = self.sr._get_pool_quota()
            if quota_stats:
                raise xs_errors.XenError('SRNoSpace',
                                       opterr='VDI size %d bytes exceeds pool quota free space %d bytes' % 
                                              (size, available))
            else:
                raise xs_errors.XenError('SRNoSpace',
                                       opterr='VDI size %d bytes exceeds pool free space %d bytes' % 
                                              (size, available))
        
        try:
            # Create RBD image with specified size and XCP-ng compatible features
            # Disable modern features that aren't supported by XCP-ng/Nautilus kernels
            cmd = self.sr._build_rbd_cmd([
                'create', 
                '--size', str(size // (1024 * 1024)),  # RBD uses MB
                '--image-format', '2',  # Use format 2 for advanced features
                '--image-feature', 'layering',  # Only enable basic layering feature
                self.rbd_name
            ])
            
            util.pread2(cmd)
            util.SMlog("Created RBD image: %s" % self.rbd_name)
            
            # Introduce VDI to XAPI database
            self._db_introduce()
            
            # Add VDI to SR and update statistics
            self.sr.vdis[self.uuid] = self
            self.sr._updateStats(sr_uuid, self.size)
            
            # Return VDI parameters for XMLRPC response
            return self.get_params()
            
        except Exception as e:
            # Clean up on failure
            try:
                cmd = self.sr._build_rbd_cmd(['rm', self.rbd_name])
                util.pread2(cmd)
            except:
                pass
            raise xs_errors.XenError('VDICreate',
                                   opterr='Failed to create RBD image: %s' % str(e))
    
    def delete(self, sr_uuid, vdi_uuid, data_only=False):
        """Delete RBD image"""
        util.SMlog("Deleting CephRBD VDI %s (data_only=%s)" % (vdi_uuid, data_only))
        
        # Check if VDI is in use - follow SM framework convention
        if hasattr(self, 'attached') and self.attached:
            raise xs_errors.XenError('VDIInUse',
                                   opterr='VDI is currently attached')
        
        # Check if RBD image is mapped anywhere and try to unmap it
        # This handles cases where the VDI was not properly detached (crashes, etc.)
        try:
            cmd = ['rbd', 'showmapped', '--format', 'json']
            output = util.pread2(cmd)
            
            if output.strip():
                mapped_devices = json.loads(output)
                
                # rbd showmapped returns a list of mapped devices
                for device_info in mapped_devices:
                    if (device_info.get('name') == self.rbd_name and 
                        device_info.get('pool') == self.sr.pool):
                        mapped_device = device_info.get('device')
                        util.SMlog("Found VDI %s mapped to device %s, attempting to unmap" % 
                                  (vdi_uuid, mapped_device))
                        try:
                            cmd = self.sr._build_rbd_cmd(['unmap', mapped_device])
                            util.pread2(cmd)
                            util.SMlog("Successfully unmapped device %s" % mapped_device)
                        except Exception as e:
                            util.SMlog("Warning: Failed to unmap device %s: %s" % (mapped_device, str(e)))
                        break
        except Exception as e:
            util.SMlog("Warning: Failed to check mapped devices: %s" % str(e))
        
        # Check for snapshots (RBD doesn't allow deletion of images with snapshots)
        try:
            cmd = self.sr._build_rbd_cmd(['snap', 'ls', self.rbd_name])
            output = util.pread2(cmd)
            
            # If output contains snapshot entries, we can't delete
            if output.strip() and 'SNAPID' in output:
                snapshots = [line for line in output.split('\n') if line.strip() and 'SNAPID' not in line]
                if snapshots:
                    raise xs_errors.XenError('VDIInUse',
                                           opterr='VDI has %d snapshots, cannot delete' % len(snapshots))
        except:
            # If snap ls fails, proceed with deletion attempt
            pass
        
        try:
            # Delete the RBD image
            cmd = self.sr._build_rbd_cmd(['rm', self.rbd_name])
            util.pread2(cmd)
            
            util.SMlog("Deleted RBD image: %s" % self.rbd_name)
            
            # Update SR allocation and statistics
            if self.uuid in self.sr.vdis:
                size_to_subtract = 0
                if hasattr(self, 'size'):
                    size_to_subtract = self.size
                del self.sr.vdis[self.uuid]
                # Update SR statistics with negative delta
                self.sr._updateStats(sr_uuid, -size_to_subtract)
                
        except Exception as e:
            # If deletion still fails due to watchers, provide a more helpful error
            error_str = str(e)
            if 'watchers' in error_str or 'still has watchers' in error_str:
                raise xs_errors.XenError('VDIInUse',
                                       opterr='VDI is still mapped on one or more hosts. Please ensure all VMs using this VDI are shut down and try again.')
            else:
                raise xs_errors.XenError('VDIDelete',
                                       opterr='Failed to delete RBD image: %s' % str(e))
    
    def attach(self, sr_uuid, vdi_uuid):
        """Attach RBD image - map to kernel device"""
        util.SMlog("Attaching CephRBD VDI %s" % self.uuid)
        
        if self.mapped:
            util.SMlog("VDI %s already mapped to %s" % (self.uuid, self.device_path))
        else:
            try:
                # Map RBD image to kernel device
                cmd = self.sr._build_rbd_cmd(['map', self.rbd_name])
                
                # Note: RBD cache options (rbd_cache, rbd_cache_size) are userspace client settings
                # and don't apply to kernel RBD mapping. XCP-ng uses kernel RBD module which
                # doesn't support these cache options.
                
                device = util.pread2(cmd).strip()
                
                # Verify device was created and is accessible
                if not os.path.exists(device):
                    raise Exception("Mapped device not found: %s" % device)
                
                self.device_path = device
                self.mapped = True
                
                util.SMlog("Mapped RBD image %s to device %s" % (self.rbd_name, device))
                
            except Exception as e:
                raise xs_errors.XenError('VDIUnavailable',
                                       opterr='Failed to map RBD image: %s' % str(e))
        
        # Set up path and xenstore data
        self.path = self.device_path
        self.attached = True
        
        if not hasattr(self, 'xenstore_data'):
            self.xenstore_data = {}
        
        # Add any RBD-specific xenstore data here if needed
        self.xenstore_data['storage-type'] = 'rbd'
        
        # Call base class attach which returns the proper XMLRPC struct
        return VDI.VDI.attach(self, sr_uuid, vdi_uuid)
    
    def detach(self, sr_uuid, vdi_uuid):
        """Detach RBD image - unmap kernel device"""
        util.SMlog("Detaching CephRBD VDI %s" % self.uuid)
        
        if not self.mapped:
            util.SMlog("VDI %s not currently mapped" % self.uuid)
            return
        
        try:
            # Unmap RBD image
            cmd = self.sr._build_rbd_cmd(['unmap', self.device_path])
            util.pread2(cmd)
            
            self.mapped = False
            self.attached = False
            
            util.SMlog("Unmapped RBD image %s from device %s" % (self.rbd_name, self.device_path))
            
        except Exception as e:
            # Log warning but don't fail - device may have been unmapped already
            util.SMlog("Warning: Failed to unmap RBD device %s: %s" % (self.device_path, str(e)))
    
    def snapshot(self, sr_uuid, vdi_uuid):
        """Create RBD snapshot and clone"""
        util.SMlog("Creating CephRBD snapshot %s -> %s" % (self.uuid, vdi_uuid))
        
        snapshot_name = "snap-%s" % vdi_uuid
        
        try:
            # Create snapshot of current RBD image
            cmd = self.sr._build_rbd_cmd(['snap', 'create', '%s@%s' % (self.rbd_name, snapshot_name)])
            util.pread2(cmd)
            
            # Protect snapshot (required for cloning)
            cmd = self.sr._build_rbd_cmd(['snap', 'protect', '%s@%s' % (self.rbd_name, snapshot_name)])
            util.pread2(cmd)
            
            # Create clone from snapshot
            clone_name = "%s%s" % (self.sr.RBD_PREFIX, vdi_uuid)
            cmd = self.sr._build_rbd_cmd([
                'clone', 
                '%s@%s' % (self.rbd_name, snapshot_name),
                clone_name
            ])
            util.pread2(cmd)
            
            # Create snapshot VDI object
            snapshot_vdi = self.sr.vdi(vdi_uuid)
            snapshot_vdi.size = self.size
            snapshot_vdi.vdi_type = self.vdi_type
            snapshot_vdi.utilisation = 0  # Clone is thin-provisioned
            snapshot_vdi.parent = self
            snapshot_vdi.rbd_name = clone_name
            
            # Add to SR
            self.sr.vdis[vdi_uuid] = snapshot_vdi
            
            util.SMlog("Created RBD snapshot and clone: %s" % clone_name)
            return snapshot_vdi
            
        except Exception as e:
            # Cleanup on failure
            try:
                # Unprotect and remove snapshot if it was created
                self.sr._build_rbd_cmd(['snap', 'unprotect', '%s@%s' % (self.rbd_name, snapshot_name)])
                self.sr._build_rbd_cmd(['snap', 'rm', '%s@%s' % (self.rbd_name, snapshot_name)])
            except:
                pass
            
            raise xs_errors.XenError('VDIClone',
                                   opterr='Failed to create RBD snapshot: %s' % str(e))
    
    def clone(self, sr_uuid, vdi_uuid):
        """Clone VDI (same as snapshot for RBD)"""
        return self.snapshot(sr_uuid, vdi_uuid)
    
    def resize(self, sr_uuid, vdi_uuid, size, online=False):
        """Resize RBD image"""
        util.SMlog("Resizing CephRBD VDI %s from %d to %d (online=%s)" % 
                   (vdi_uuid, self.size, size, online))
        
        if size < self.size:
            raise xs_errors.XenError('VDIShrink',
                                   opterr='Cannot shrink RBD images')
        
        if size == self.size:
            return
        
        try:
            # Resize RBD image
            cmd = self.sr._build_rbd_cmd([
                'resize',
                '--size', str(size // (1024 * 1024)),  # RBD uses MB
                self.rbd_name
            ])
            
            util.pread2(cmd)
            
            # Update size tracking
            old_size = self.size
            self.size = size
            size_delta = size - old_size
            self.sr._updateStats(sr_uuid, size_delta)
            
            util.SMlog("Resized RBD image %s to %d bytes" % (self.rbd_name, size))
            
        except Exception as e:
            raise xs_errors.XenError('VDISize',
                                   opterr='Failed to resize RBD image: %s' % str(e))
    
    def activate(self, sr_uuid, vdi_uuid):
        """Activate VDI (ensure it's mapped)"""
        if not self.mapped:
            return self.attach(sr_uuid, vdi_uuid)
        return self.device_path
    
    def deactivate(self, sr_uuid, vdi_uuid):
        """Deactivate VDI (unmap if not attached)"""
        if not self.attached and self.mapped:
            self.detach(sr_uuid, vdi_uuid)
    
    def _load_from_rbd_info(self, rbd_info):
        """Load VDI properties from RBD ls output"""
        self.size = rbd_info.get('size', 0)
        
        # RBD images are thin-provisioned, so utilisation is initially 0
        # We could query 'rbd disk-usage' for actual usage but it's expensive
        self.utilisation = 0
        
        # Check if image is currently mapped
        try:
            cmd = ['rbd', 'showmapped', '--format', 'json']
            output = util.pread2(cmd)
            
            if output.strip():
                mapped_devices = json.loads(output)
                
                # rbd showmapped returns a list of mapped devices
                for device_info in mapped_devices:
                    if (device_info.get('name') == self.rbd_name and 
                        device_info.get('pool') == self.sr.pool):
                        self.device_path = device_info.get('device', self.device_path)
                        self.mapped = True
                        break
        except:
            # If we can't determine mapping status, assume not mapped
            self.mapped = False


# Register the driver with the SR framework
if __name__ == '__main__':
    SRCommand.run(CephRBDSR, DRIVER_INFO)
else:
    SR.registerSR(CephRBDSR)
