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
import datetime
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
    ['user', 'Ceph user name (optional, default admin)'],
    ['prefix', 'Prefix for RBD image names (optional)']
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
    "MULTIPATH": False  # Ceph is natively parallel
}

class CephRBDSR(SR.SR):
    """Ceph RBD Storage Repository implementation"""
    
    DRIVER_TYPE = "cephrbd"
    RBD_PREFIX = "vdi-"  # Prefix for RBD image names, there is also "conf.prefix" which is configurable and allows using same pool for multiple SRs with different prefixes
    SNAP_PREFIX = "snap-"
    
    def handles(type):
        """Check if this driver handles the given storage type"""
        return type == CephRBDSR.DRIVER_TYPE
    handles = staticmethod(handles)
    
    def load(self, sr_uuid):
        """Load and validate SR configuration"""
        util.SMlog("Loading CephRBDSR %s" % sr_uuid)
        
        # Validate required configuration
        if 'pool' not in self.dconf:
            raise xs_errors.XenError('ConfigParameterMissing', opterr='Missing required parameter: pool')
        
        # Store configuration
        self.pool = self.dconf['pool']
        self.ceph_conf = self.dconf.get('conf', '/etc/ceph/ceph.conf')
        self.ceph_user = self.dconf.get('user', 'admin')
        self.keyring = self.dconf.get('keyring', '')
        self.mon_host = self.dconf.get('mon_host', '')
        self.prefix = self.dconf.get('prefix', '')
        
        # Validate Ceph configuration file exists
        if not os.path.exists(self.ceph_conf):
            raise xs_errors.XenError('ConfigParameterInvalid', opterr='Ceph config file not found: %s' % self.ceph_conf)
        
        # Set up paths and properties
        self.path = "/dev/rbd/%s" % self.pool  # Kernel RBD device path
        self.driver_config = DRIVER_CONFIG
        
        # Initialize locking
        self.lock = Lock("CephRBDSR", sr_uuid)
        
        util.SMlog("CephRBDSR loaded successfully: pool=%s" % self.pool)
    
    def create(self, sr_uuid, size):
        """Create new Ceph RBD storage repository"""
        util.SMlog("Creating CephRBDSR %s with size %d in pool %s" % (sr_uuid, size, self.pool))
        
        # Verify pool exists and is accessible
        try:
            cmd = self._build_ceph_cmd(['osd', 'pool', 'stats', self.pool])
            util.pread2(cmd)
            util.SMlog("Verified pool %s exists" % self.pool)
        except Exception as e:
            raise xs_errors.XenError('SRUnavailable', opterr='Cannot access Ceph pool %s: %s' % (self.pool, str(e)))
        
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
                    if image_name.startswith(self.prefix + self.RBD_PREFIX):
                        try:
                            if ('snapshot' in image_info):
                                snapshot_id = image_info['snapshot']
                                vdi_uuid = snapshot_id[len(self.prefix + self.SNAP_PREFIX):]
                                if (self._is_valid_uuid(vdi_uuid)):
                                    parent_id = image_info['image']
                                    vdi = self.vdi(vdi_uuid)
                                    vdi._load_from_rbd_info(image_info)
                                    # If the snapshot_of is missing, fix it
                                    if (vdi.snapshot_of is None):
                                        vdi.snapshot_of = parent_id[len(self.prefix + self.RBD_PREFIX):]
                                        util.SMlog("Fixed missing snapshot_of for VDI %s: set to %s" % (vdi_uuid, vdi.snapshot_of))
                                    self.vdis[snapshot_id] = vdi
                                    self.virtual_allocation += vdi.size
                                    util.SMlog("Found VDI snapshot of %s: %s (size: %d)" % (parent_id, snapshot_id, vdi.size))
                            else:
                                vdi_uuid = image_name[len(self.prefix + self.RBD_PREFIX):]
                                if (self._is_valid_uuid(vdi_uuid)):
                                    vdi = self.vdi(vdi_uuid)
                                    vdi._load_from_rbd_info(image_info)
                                    self.vdis[vdi_uuid] = vdi
                                    self.virtual_allocation += vdi.size
                                    util.SMlog("Found VDI: %s (size: %d)" % (vdi_uuid, vdi.size))

                        except Exception as e:
                            util.SMlog("Error loading VDI %s: %s" % (vdi_uuid, str(e)))
                
                util.SMlog("Scan complete: found %d VDIs, total allocation: %d" % (len(self.vdis), self.virtual_allocation))
            
        except Exception as e:
            util.SMlog("Error during scan: %s" % str(e))
            raise xs_errors.XenError('SRScanError', opterr=str(e))
        
        return super(CephRBDSR, self).scan(sr_uuid)
    
    def vdi(self, uuid):
        """Create VDI object for given UUID"""
        return CephRBDVDI(self, uuid)

    def stat(self, sr_uuid):
        """Update SR capacity/usage statistics and sync with XAPI."""
        util.SMlog("CephRBDSR.stat: updating statistics for SR %s" % sr_uuid)
        
        s = self._get_pool_stats()
        total = s.get('total', 0)
        used = s.get('used', 0)

        # Always update physical statistics from Ceph
        self.physical_size = total
        self.physical_utilisation = used

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
            
        self.physical_size = int(total)
        self.physical_utilisation = int(used)
        
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
                util.SMlog("CephRBDSR: using pool quota: total=%d used=%d" %  (quota_stats['total'], quota_stats['used']))
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
                    used  = int((stats.get('stored') if 'stored' in stats else stats.get('bytes_used', 0)) or 0)
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
        # On first sight it may appear as this is identical to attached, but it's not, attached is a state when VDI is attached in XAPI
        # Mapped is a state when RBD image is mapped in the kernel. Ideally these should be in sync, but in case of crashes or bugs they might differ
        self.mapped = False
        # When the image is mapped we store the device to sm config so that when we need to detach it, we are referring to the correct device
        # in theory you can map same RBD image multiple times and get different device paths, so we need to make sure we unmap the correct one
        self.mapped_path_known = False
        self.mapped_path = None
        self.snapshot_of = None
        self.is_protected = False
        self.is_a_snapshot = False
        VDI.VDI.__init__(self, sr, uuid)
        # Set VDI type - RBD provides raw block devices
        self.vdi_type = vhdutil.VDI_TYPE_RAW  # 'aio'
        if (self.is_a_snapshot):
            self.rbd_name = "%s%s%s" % (sr.prefix, sr.SNAP_PREFIX, uuid)
            self.device_path = None
        else:
            self.rbd_name = "%s%s%s" % (sr.prefix, sr.RBD_PREFIX, uuid)
            self.device_path = "/dev/rbd/%s/%s" % (sr.pool, self.rbd_name)

        if not hasattr(self, 'sm_config'):
            self.sm_config = {}
    
    def _load_if_exists(self, vdi_uuid):
        """Load VDI properties from XAPI database only if VDI exists in XAPI"""
        try:
            # Check if VDI exists in XAPI database
            vdi_ref = self.sr.session.xenapi.VDI.get_by_uuid(vdi_uuid)
            
            # VDI exists, load its properties
            util.SMlog("Loading existing VDI properties from XAPI database for %s" % vdi_uuid)
            
            # Call parent load method first  
            VDI.VDI.load(self, vdi_uuid)
            
            # Load key properties from XAPI database
            self.is_a_snapshot = self.sr.session.xenapi.VDI.get_is_a_snapshot(vdi_ref)
            self.read_only = self.sr.session.xenapi.VDI.get_read_only(vdi_ref)
            self.size = int(self.sr.session.xenapi.VDI.get_virtual_size(vdi_ref))
            self.utilisation = int(self.sr.session.xenapi.VDI.get_physical_utilisation(vdi_ref))
            
            # Load sm_config to get RBD-specific information
            self.sm_config = self.sr.session.xenapi.VDI.get_sm_config(vdi_ref)
            
            # Restore the device path if it was stored during attach (using host-specific key)
            host_ref = self.sr.session.xenapi.host.get_by_uuid(util.get_this_host())
            host_key = "host_%s_device_path" % host_ref
            if host_key in self.sm_config:
                self.attached = True
                self.mapped = True
                self.mapped_path_known = True
                self.mapped_path = self.sm_config[host_key]
                util.SMlog("Restored mapped path from sm_config for host %s: %s" % (host_ref, self.mapped_path))
            else:
                util.SMlog("Key not found: %s" % host_key)
            
            # Extract snapshot name and RBD name from sm_config if it's a snapshot
            if self.is_a_snapshot:
                snapshot_of_uuid = self.sm_config.get('snapshot_of', '')
                snapshot_name = self.sr.SNAP_PREFIX + vdi_uuid
                
                if snapshot_of_uuid:
                    # For snapshots, rbd_name is parent_image@snapshot_name
                    parent_rbd_name = "%s%s%s" % (self.sr.prefix, self.sr.RBD_PREFIX, snapshot_of_uuid)
                    self.rbd_name = "%s@%s" % (parent_rbd_name, snapshot_name)
                    self.snapshot_of = snapshot_of_uuid
                    util.SMlog("Loaded snapshot VDI: %s -> RBD name: %s" % (vdi_uuid, self.rbd_name))
                else:
                    util.SMlog("Warning: Snapshot VDI %s missing snapshot_of in sm_config" % vdi_uuid)
            
            util.SMlog("Loaded existing VDI %s: is_snapshot=%s, read_only=%s, size=%d" % (vdi_uuid, self.is_a_snapshot, self.read_only, self.size))
            return True
            
        except Exception as e:
            # VDI doesn't exist in XAPI database - this is normal for newly discovered RBD images
            util.SMlog("VDI %s not found in XAPI database (new discovery): %s" % (vdi_uuid, str(e)))
            # Keep the default values set in __init__
            return False

    def load(self, vdi_uuid):
        return self._load_if_exists(vdi_uuid)
    
    def create(self, sr_uuid, vdi_uuid, size):
        """Create new RBD image"""
        util.SMlog("Creating CephRBD VDI %s, size %d" % (vdi_uuid, size))
        
        # Set VDI properties
        self.uuid = vdi_uuid
        self.location = vdi_uuid  # For RBD, location is the VDI UUID
        self.size = size
        self.utilisation = 0  # Ceph uses thin provisioning
        self.vdi_type = vhdutil.VDI_TYPE_RAW  # RBD provides raw block devices
        
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
            raise xs_errors.XenError('VDICreate', opterr='Failed to create RBD image: %s' % str(e))
    
    def _parent_name(self):
        """Get parent image name if this VDI is a snapshot"""
        if self.is_a_snapshot and self.snapshot_of:
            return "%s%s%s" % (self.sr.prefix, self.sr.RBD_PREFIX, self.snapshot_of)
        return None

    def _try_find_parent(self):
        try:
            # Check if parent image exists in the pool
            cmd = self.sr._build_rbd_cmd(['ls', '-l', '--format', 'json'])
            output = util.pread2(cmd)
            if (output.strip()):
                data = json.loads(output)
                for image_info in data:
                    if image_info['snapshot'] == self.rbd_name:
                        return image_info['image']
            
        except Exception as e:
            util.SMlog("Parent image for snapshot %s not found: %s" % (self.rbd_name, str(e)))
            return None

    def delete(self, sr_uuid, vdi_uuid, data_only=False):
        """Delete RBD image"""
        util.SMlog("Deleting CephRBD VDI %s (data_only=%s)" % (vdi_uuid, data_only))
        util.SMlog("Is a snapshot: %s" % self.is_a_snapshot)
        
        # Check if VDI is in use - follow SM framework convention
        if hasattr(self, 'attached') and self.attached:
            raise xs_errors.XenError('VDIInUse', opterr='VDI is currently attached')
        
        # Check if RBD image is mapped anywhere and try to unmap it
        # This handles cases where the VDI was not properly detached (crashes, etc.)
        try:
            self._verify_mapping()

            if (self.mapped):
                try:
                    cmd = self.sr._build_rbd_cmd(['unmap', self.device_path])
                    util.pread2(cmd)
                    util.SMlog("Successfully unmapped device %s" % self.device_path)
                except Exception as e:
                    util.SMlog("Warning: Failed to unmap device %s: %s" % (self.device_path, str(e)))
        except Exception as e:
            util.SMlog("Warning: Failed to check mapped devices: %s" % str(e))
        
        # If this is a snapshot, check if it's protected
        if (self.is_a_snapshot):
            if (self.is_protected):
                raise xs_errors.XenError('VDIInUse', opterr='VDI is a protected snapshot, cannot delete')
        else:
            # Check for snapshots (RBD doesn't allow deletion of images with snapshots)
            try:
                # Get a JSON list of snapshots for this image
                cmd = self.sr._build_rbd_cmd(['snap', 'ls', self.rbd_name, '--format', 'json'])
                output = util.pread2(cmd)

                if output.strip():
                    snapshot_data = json.loads(output)
                    snapshot_count = len(snapshot_data)
                
                    if snapshot_count > 0:
                        raise xs_errors.XenError('VDIInUse', opterr='VDI has %d snapshots, cannot delete' % snapshot_count)
            except Exception as e:
                util.SMlog("Warning: Failed to check for snapshots: %s" % str(e))
        
        try:
            # Check if this VDI represents a snapshot (rbd_name contains @)
            if self.is_a_snapshot:
                # This is a snapshot, unprotect and delete it
                util.SMlog("Deleting RBD snapshot: %s" % self.rbd_name)

                # In very rare cases, such as when snapshot is discovered during scan, the snapshot_of may be missing, if that's the case we fix it
                if (self.snapshot_of is None):
                    self.snapshot_of = self._try_find_parent()
                    if (self.snapshot_of is None):
                        raise xs_errors.XenError('VDIDelete', opterr='Snapshot missing snapshot_of and parent image not found')
                    else:
                        util.SMlog("Fixed missing snapshot_of for snapshot %s: set to %s" % (self.rbd_name, self.snapshot_of))

                try:
                    # First try to unprotect the snapshot
                    cmd = self.sr._build_rbd_cmd(['snap', 'unprotect', '--image', self._parent_name(), '--snap', self.rbd_name])
                    util.pread2(cmd)
                    util.SMlog("Unprotected snapshot: %s" % self.rbd_name)
                except Exception as e:
                    # If unprotect fails, the snapshot might not be protected or might have children
                    util.SMlog("Warning: Failed to unprotect snapshot %s: %s" % (self.rbd_name, str(e)))
                    # Check if snapshot has children (clones)
                    image_name, snap_name = self.rbd_name.split('@', 1)
                    try:
                        cmd = self.sr._build_rbd_cmd(['children', self.rbd_name])
                        children = util.pread2(cmd)
                        if children.strip():
                            raise xs_errors.XenError('VDIInUse',
                                                   opterr='Snapshot has children (clones), cannot delete: %s' % children.strip())
                    except Exception:
                        pass  # children command might fail if no children
                
                # Delete the snapshot
                cmd = self.sr._build_rbd_cmd(['snap', 'rm', '--image', self._parent_name(), '--snap', self.rbd_name])
                util.pread2(cmd)
                util.SMlog("Deleted RBD snapshot: %s" % self.rbd_name)
                
            else:
                # This is a regular RBD image
                util.SMlog("Deleting RBD image: %s" % self.rbd_name)
                
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
            # Handle different types of deletion errors
            error_str = str(e)
            
            if 'watchers' in error_str or 'still has watchers' in error_str:
                raise xs_errors.XenError('VDIInUse', opterr='VDI is still mapped on one or more hosts. Please ensure all VMs using this VDI are shut down and try again.')
            elif 'No such file or directory' in error_str:
                # RBD image/snapshot doesn't exist - this might be an orphaned XAPI VDI entry
                util.SMlog("RBD image/snapshot %s does not exist - might be orphaned XAPI entry, cleaning up VDI record" % self.rbd_name)
                # Still remove from SR vdis list to clean up
                if self.uuid in self.sr.vdis:
                    del self.sr.vdis[self.uuid]
                # This is not really an error - the VDI is effectively deleted
                util.SMlog("Successfully cleaned up orphaned VDI entry: %s" % self.uuid)
            else:
                raise xs_errors.XenError('VDIDelete', opterr='Failed to delete RBD image/snapshot: %s' % str(e))
    
    def attach(self, sr_uuid, vdi_uuid):
        """Attach RBD image or snapshot - map to kernel device"""
        util.SMlog("Attaching CephRBD VDI %s" % self.uuid)

        try:
            # Map RBD image or snapshot to kernel device
            # For snapshots, rbd_name will be in format "image@snap-name"
            # For regular images, rbd_name will be just "image-name"
            cmd = self.sr._build_rbd_cmd(['map', self.rbd_name])
            
            device = util.pread2(cmd).strip()
            
            # Verify device was created and is accessible
            if not os.path.exists(device):
                raise Exception("Mapped device not found: %s" % device)
            
            # Store the exact device path that was assigned to this VDI instance
            self.device_path = device
            self.mapped = True
            
            util.SMlog("Mapped RBD image %s to device %s" % (self.rbd_name, device))
            
        except Exception as e:
            raise xs_errors.XenError('VDIUnavailable', opterr='Failed to map RBD image: %s' % str(e))
        
        # Set up path and xenstore data
        self.path = self.device_path
        self.attached = True

        # Store the device path in VDI sm_config with host-specific key for migration resilience
        # Use host-specific key to support multiple simultaneous attachments during migration
        host_ref = self.sr.session.xenapi.host.get_by_uuid(util.get_this_host())
        host_key = "host_%s_device_path" % host_ref
        
        # Use direct XAPI call to add host-specific key (bypasses _db_update filtering)
        vdi_ref = self.sr.session.xenapi.VDI.get_by_uuid(self.uuid)
        self.sr.session.xenapi.VDI.add_to_sm_config(vdi_ref, host_key, self.device_path)
        
        util.SMlog("Stored device path %s for host %s in sm_config key %s" % (self.device_path, host_ref, host_key))
        
        # Call base class attach which returns the proper XMLRPC struct
        return VDI.VDI.attach(self, sr_uuid, vdi_uuid)
    
    def detach(self, sr_uuid, vdi_uuid):
        """Detach RBD image - unmap kernel device"""
        util.SMlog("Detaching CephRBD VDI %s" % self.uuid)
        
        if not self.mapped_path_known:
            # Show error here - we can't continue
            raise xs_errors.XenError('VDINotAttached', opterr='VDI was not properly attached, cannot detach safely')

        self.device_path = self.mapped_path

        try:
            cmd = self.sr._build_rbd_cmd(['unmap', self.device_path])
            util.pread2(cmd)
            
            self.mapped = False
            self.attached = False
            
            # Clean up the host-specific device path from sm_config
            host_ref = self.sr.session.xenapi.host.get_by_uuid(util.get_this_host())
            host_key = "host_%s_device_path" % host_ref
            
            # Use direct XAPI call to remove host-specific key
            vdi_ref = self.sr.session.xenapi.VDI.get_by_uuid(self.uuid)
            current_sm_config = self.sr.session.xenapi.VDI.get_sm_config(vdi_ref)
            if host_key in current_sm_config:
                self.sr.session.xenapi.VDI.remove_from_sm_config(vdi_ref, host_key)
                util.SMlog("Cleaned up device path for host %s from sm_config" % host_ref)
            else:
                util.SMlog("Device path key %s not found in sm_config, nothing to clean up" % host_key)
            
            util.SMlog("Unmapped RBD image %s from device %s" % (self.rbd_name, self.device_path))
            
        except Exception as e:
            # Log warning but don't fail - device may have been unmapped already
            util.SMlog("Warning: Failed to unmap RBD device %s: %s" % (self.device_path, str(e)))
    
    def snapshot(self, sr_uuid, vdi_uuid):
        """Create RBD snapshot (read-only)"""
        util.SMlog("Creating CephRBD snapshot from VDI %s" % self.uuid)
        
        # Generate new UUID for the snapshot
        snapshot_uuid = util.gen_uuid()
        util.SMlog("Generated new UUID for snapshot: %s" % snapshot_uuid)

        snapshot_name = "snap-%s" % snapshot_uuid
        
        try:
            # Create snapshot of current RBD image
            cmd = self.sr._build_rbd_cmd(['snap', 'create', '%s@%s' % (self.rbd_name, snapshot_name)])
            util.pread2(cmd)
            
            # Protect snapshot (makes it read-only and prevents deletion)
            cmd = self.sr._build_rbd_cmd(['snap', 'protect', '%s@%s' % (self.rbd_name, snapshot_name)])
            util.pread2(cmd)
            
            # Create snapshot VDI object with the new UUID - this represents the read-only snapshot
            snapshot_vdi = self.sr.vdi(snapshot_uuid)
            snapshot_vdi.size = self.size
            snapshot_vdi.vdi_type = self.vdi_type
            snapshot_vdi.utilisation = 0  # Snapshot is thin-provisioned
            snapshot_vdi.parent = self
            # Store the snapshot reference instead of RBD image name
            snapshot_vdi.rbd_name = "%s@%s" % (self.rbd_name, snapshot_name)
            
            # Set additional VDI properties for proper database introduction
            snapshot_vdi.location = snapshot_uuid
            snapshot_vdi.read_only = True  # Snapshots are read-only
            snapshot_vdi.label = self.label + " (snapshot)"
            snapshot_vdi.description = "Snapshot of " + self.description
            snapshot_vdi.ty = "user"  # VDI type for XAPI
            snapshot_vdi.shareable = False
            snapshot_vdi.managed = True
            snapshot_vdi.is_a_snapshot = True  # This IS a snapshot
            snapshot_vdi.metadata_of_pool = ""
            snapshot_vdi.snapshot_time = datetime.datetime.utcnow().strftime("%Y%m%dT%H:%M:%SZ")  # Current timestamp in ISO format
            snapshot_vdi.snapshot_of = self.uuid  # Reference to parent VDI
            snapshot_vdi.cbt_enabled = False
            
            # Initialize sm_config
            if not hasattr(snapshot_vdi, 'sm_config'):
                snapshot_vdi.sm_config = {}
            snapshot_vdi.sm_config['vdi_type'] = self.vdi_type
            # Mark this as a snapshot in sm_config
            snapshot_vdi.sm_config['snapshot'] = 'true'
            snapshot_vdi.sm_config['snapshot_of'] = self.uuid
            snapshot_vdi.sm_config['snapshot_name'] = snapshot_name  # Store snapshot name for later retrieval
            
            # Introduce the new VDI to XAPI database
            vdi_ref = snapshot_vdi._db_introduce()
            util.SMlog("vdi_snapshot: introduced VDI: %s (%s)" % (vdi_ref, snapshot_uuid))
            
            # Add to SR with the new UUID
            self.sr.vdis[snapshot_uuid] = snapshot_vdi
            
            util.SMlog("Created RBD snapshot: %s (UUID: %s)" % (snapshot_vdi.rbd_name, snapshot_uuid))
            return snapshot_vdi.get_params()
            
        except Exception as e:
            # Cleanup on failure
            try:
                # Unprotect and remove snapshot if it was created
                cmd = self.sr._build_rbd_cmd(['snap', 'unprotect', '%s@%s' % (self.rbd_name, snapshot_name)])
                util.pread2(cmd)
                cmd = self.sr._build_rbd_cmd(['snap', 'rm', '%s@%s' % (self.rbd_name, snapshot_name)])
                util.pread2(cmd)
            except:
                pass
            
            raise xs_errors.XenError('VDISnapshot',
                                   opterr='Failed to create RBD snapshot: %s' % str(e))
    
    def clone(self, sr_uuid, vdi_uuid):
        """Clone VDI (create writable copy from snapshot)"""
        util.SMlog("Creating CephRBD clone from VDI %s" % self.uuid)
        
        # Generate new UUID for the clone
        clone_uuid = util.gen_uuid()
        util.SMlog("Generated new UUID for clone: %s" % clone_uuid)

        clone_name = "%s%s%s" % (self.sr.prefix, self.sr.RBD_PREFIX, clone_uuid)
        snapshot_name = "snap-clone-%s" % clone_uuid
        
        try:
            # Create snapshot of current RBD image
            cmd = self.sr._build_rbd_cmd(['snap', 'create', '%s@%s' % (self.rbd_name, snapshot_name)])
            util.pread2(cmd)
            
            # Protect snapshot (required for cloning)
            cmd = self.sr._build_rbd_cmd(['snap', 'protect', '%s@%s' % (self.rbd_name, snapshot_name)])
            util.pread2(cmd)
            
            # Create clone from snapshot (this creates a new writable RBD image)
            cmd = self.sr._build_rbd_cmd([
                'clone', 
                '%s@%s' % (self.rbd_name, snapshot_name),
                clone_name
            ])
            util.pread2(cmd)
            
            # Create clone VDI object with the new UUID
            clone_vdi = self.sr.vdi(clone_uuid)
            clone_vdi.size = self.size
            clone_vdi.vdi_type = self.vdi_type
            clone_vdi.utilisation = 0  # Clone is thin-provisioned
            clone_vdi.parent = self
            clone_vdi.rbd_name = clone_name
            
            # Set additional VDI properties for proper database introduction
            clone_vdi.location = clone_uuid
            clone_vdi.read_only = False  # Clone is writable
            clone_vdi.label = self.label + " (clone)"
            clone_vdi.description = "Clone of " + self.description
            clone_vdi.ty = "user"  # VDI type for XAPI
            clone_vdi.shareable = False
            clone_vdi.managed = True
            clone_vdi.is_a_snapshot = False  # Clone is not a snapshot, it's a new VDI
            clone_vdi.metadata_of_pool = ""
            clone_vdi.snapshot_time = "19700101T00:00:00Z"
            clone_vdi.snapshot_of = ""
            clone_vdi.cbt_enabled = False
            
            # Initialize sm_config
            if not hasattr(clone_vdi, 'sm_config'):
                clone_vdi.sm_config = {}
            clone_vdi.sm_config['vdi_type'] = self.vdi_type
            # Mark the parent snapshot info in sm_config 
            clone_vdi.sm_config['clone_of'] = self.uuid
            clone_vdi.sm_config['parent_snapshot'] = "%s@%s" % (self.rbd_name, snapshot_name)
            
            # Introduce the new VDI to XAPI database
            vdi_ref = clone_vdi._db_introduce()
            util.SMlog("vdi_clone: introduced VDI: %s (%s)" % (vdi_ref, clone_uuid))
            
            # Add to SR with the new UUID
            self.sr.vdis[clone_uuid] = clone_vdi
            
            util.SMlog("Created RBD clone: %s (UUID: %s)" % (clone_name, clone_uuid))
            return clone_vdi.get_params()
            
        except Exception as e:
            # Cleanup on failure
            try:
                # Remove clone if it was created
                cmd = self.sr._build_rbd_cmd(['rm', clone_name])
                util.pread2(cmd)
            except:
                pass
                
            try:
                # Unprotect and remove snapshot if it was created
                cmd = self.sr._build_rbd_cmd(['snap', 'unprotect', '%s@%s' % (self.rbd_name, snapshot_name)])
                util.pread2(cmd)
                cmd = self.sr._build_rbd_cmd(['snap', 'rm', '%s@%s' % (self.rbd_name, snapshot_name)])
                util.pread2(cmd)
            except:
                pass
            
            raise xs_errors.XenError('VDIClone',
                                   opterr='Failed to create RBD clone: %s' % str(e))
    
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
    
    def _verify_mapping(self):
        # Check if RBD is mapped to block device, and if our internal mapping info is up-to-date
        cmd = ['rbd', 'showmapped', '--format', 'json']
        output = util.pread2(cmd)
        
        if output.strip():
            mapped_devices = json.loads(output)
            
            # If we already have a specific device_path tracked, verify it's still valid
            if hasattr(self, 'device_path') and self.device_path:
                for device_info in mapped_devices:
                    if (device_info.get('name') == self.rbd_name and device_info.get('pool') == self.sr.pool):
                        if device_info.get('device') == self.device_path:
                            self.mapped = True
                            util.SMlog("Verified tracked device %s is still mapped for %s" % (self.device_path, self.rbd_name))
                        else:
                            util.SMlog("VDI %s expected at %s found at %s - updating mapping" % (self.rbd_name, self.device_path, device_info.get('device')))
                            self.device_path = device_info.get('device')
                            self.mapped = True
                        return
                        
                # Our tracked device is no longer valid, clear it
                util.SMlog("RBD for %s is not currently mapped (expected at %s)" % (self.rbd_name, self.device_path))
                self.mapped = False
            else:
                # No specific device tracked, find any mapping (for initial discovery)
                for device_info in mapped_devices:
                    if (device_info.get('name') == self.rbd_name and 
                        device_info.get('pool') == self.sr.pool):
                        self.device_path = device_info.get('device', self.device_path)
                        self.mapped = True
                        util.SMlog("Discovered %s mapped to %s during scan" % (self.rbd_name, self.device_path))
                        return
                self.mapped = False
        else:
            # No mapped devices found
            self.mapped = False

    def _load_from_rbd_info(self, rbd_info):
        """Load VDI properties from RBD ls output"""
        self.size = rbd_info.get('size', 0)
        
        # RBD images are thin-provisioned, so utilisation is initially 0
        # We could query 'rbd disk-usage' for actual usage but it's expensive
        self.utilisation = 0

        if ('snapshot' in rbd_info):
            self.is_a_snapshot = True
            # Snapshots in CEPH are always read-only
            self.read_only = True
            self.parent_uuid = rbd_info['image']
        
        try:
            self._verify_mapping()
                
        except Exception as e:
            # If we can't determine mapping status, assume not mapped
            util.SMlog("Warning: Failed to check mapping status for %s: %s" % (self.rbd_name, str(e)))
            self.mapped = False


# Register the driver with the SR framework
if __name__ == '__main__':
    SRCommand.run(CephRBDSR, DRIVER_INFO)
else:
    SR.registerSR(CephRBDSR)
