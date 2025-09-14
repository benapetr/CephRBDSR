# CephRBDSR - Ceph RBD Storage Repository Driver for XCP-ng

NOTE: This code is still work in progress - don't use in production yet

SMAPIv1 storage drivers that enables XCP-ng to use Ceph RADOS Block Device (RBD) images as virtual disk storage, providing distributed, scalable, and highly available block storage.

This repository contains 2 SM drivers: CephRBDSR and LVMoRBD

CephRBDSR is an attempt to bring native RBD support to XCP-ng, allowing each VM to have disks provisioned as individual RBD images utilizing all low-level
storage features of CEPH (fast snapshots, easy backups, etc.)

LVMoRBD is a wrapper SR driver that is meant to simplify creation of LVMSR on top of RBD image.

## Features

- **SMAPIv1 Compliance**: Full integration with XCP-ng's Storage Manager framework
- **Thin Provisioning**: Efficient space utilization with Ceph's thin provisioning
- **High Availability**: Leverages Ceph's distributed architecture and replication
- **Live Operations**: Support for online resize, snapshots, and cloning
- **Pool Quota Support**: Respects Ceph pool quotas when configured (CephRBDSR works with pool quotas, LVMoRBD uses RBD image as total capacity reference)
- **XCP-ng Compatibility**: Optimized for XCP-ng's Nautilus-era kernel
- **Robust Error Handling**: Comprehensive error handling and recovery
- **Statistics Tracking**: Real-time pool usage and capacity reporting

## Supported Operations

- ✅ **VDI Create/Delete**: Create and delete RBD images
- ✅ **VDI Attach/Detach**: Map/unmap RBD devices to/from hosts
- ✅ **VDI Resize**: Online and offline disk expansion
- ✅ **VDI Clone**: Copy-on-write cloning via RBD snapshots
- ✅ **VDI Snapshot**: Create point-in-time snapshots
- ✅ **SR Probe**: Discover existing Ceph pools
- ✅ **Live Migration**: VDI migration between hosts
- ✅ **Pool Quotas**: Honor Ceph pool quota limits

## Requirements

### Ceph Cluster
- **Ceph version**: Nautilus or later (tested with Nautilus runtime (directly on XCP-ng dom0) on Octopus CEPH cluster)
- **RBD kernel module**: Available on XCP-ng hosts
- **Pool configuration**: Pre-configured Ceph pool for storage

### XCP-ng Host
- **XCP-ng version**: 8.2+ 
- **Python**: 2.7 (XCP-ng default)
- **Ceph client tools**: `ceph` and `rbd` commands installed
- **Network access**: Connectivity to Ceph monitors

## Installation

### 1. Install Ceph Client Tools

```bash
# Install Ceph repository (on dom0)
/etc/yum.repos.d/ceph.repo:

[ceph]
name=CEPH
baseurl=https://download.ceph.com/rpm-nautilus/el7/x86_64/
enabled=1
gpgcheck=0

# Install Ceph client
yum install ceph-common
```

### 2. Configure Ceph Authentication

```bash
# Copy Ceph configuration and keyring to XCP-ng host
scp user@ceph-admin:/etc/ceph/ceph.conf /etc/ceph/
scp user@ceph-admin:/etc/ceph/ceph.client.admin.keyring /etc/ceph/

# Test connectivity
ceph status
ceph osd pool ls
```

### 3. Install the Driver

```bash
# Clone the repository
git clone https://github.com/your-org/CephRBDSR.git
cd CephRBDSR

# Install the driver
chmod +x install_cephrbd.sh
./install_cephrbd.sh

# Restart XAPI to load the new driver
systemctl restart xapi
```

### 4. Verify Installation

```bash
# Check if driver is registered
xe sm-list | grep -i ceph

# Check driver capabilities
xe sm-list uuid=<sr-uuid> params=capabilities
```

## Configuration

### Storage Repository Creation

#### Via XenCenter/XCP-ng Center
1. Add new Storage Repository
2. Select "Ceph RBD Storage" from the list
3. Configure the following parameters:
   - **Pool Name**: Name of the Ceph pool (required)
   - **Ceph Config**: Path to ceph.conf (optional, defaults to `/etc/ceph/ceph.conf`)
   - **Ceph User**: Authentication user (optional, defaults to `admin`)

#### Via CLI
```bash
xe sr-create \
  host-uuid=<host-uuid> \
  type=cephrbd \
  name-label="Ceph RBD Storage" \
  device-config:pool=rbd \
  device-config:ceph_conf=/etc/ceph/ceph.conf \
  device-config:ceph_user=admin
```

### Configuration Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `pool` | ✅ Yes | - | Ceph pool name for RBD images |
| `ceph_conf` | No | `/etc/ceph/ceph.conf` | Path to Ceph configuration file |
| `ceph_user` | No | `admin` | Ceph authentication user |
| `keyring` | No | - | Custom keyring path (optional) |
| `mon_host` | No | - | Monitor hosts (optional, overrides config) |

### Pool Quota Configuration

The driver supports Ceph pool quotas to limit storage usage:

```bash
# Set a 100GB quota on the pool
ceph osd pool set-quota rbd max_bytes 107374182400

# Remove quota
ceph osd pool set-quota rbd max_bytes 0

# Check quota status
ceph osd pool get-quota rbd
```

When a quota is set, XCP-ng will show the quota size as the total SR capacity instead of the full cluster capacity.

## Architecture

### Component Overview
```
┌──────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   XCP-ng Host    │    │   Ceph Cluster   │    │   RBD Images    │
│                  │    │                  │    │                 │
│  ┌─────────────┐ │    │  ┌─────────────┐ │    │ ┌─────────────┐ │
│  │ XAPI/SMAPI  │ │    │  │   Monitor   │ │    │ │ vdi-uuid-1  │ │
│  └─────────────┘ │    │  └─────────────┘ │    │ └─────────────┘ │
│  ┌─────────────┐ │    │  ┌─────────────┐ │    │ ┌─────────────┐ │
│  │ CephRBDSR.py│ │◄──►│  │     OSD     │ │◄──►│ │ vdi-uuid-2  │ │
│  └─────────────┘ │    │  └─────────────┘ │    │ └─────────────┘ │
│  ┌─────────────┐ │    │  ┌─────────────┐ │    │ ┌─────────────┐ │
│  │   /dev/rbd0 │ │    │  │     OSD     │ │    │ │ vdi-uuid-n  │ │
│  └─────────────┘ │    │  └─────────────┘ │    │ └─────────────┘ │
└──────────────────┘    └──────────────────┘    └─────────────────┘
```

### Storage Flow
1. **VDI Creation**: Driver creates RBD image in configured pool
2. **VDI Attachment**: RBD image mapped to kernel block device (`/dev/rbdX`)
3. **VM Access**: Hypervisor uses block device for VM disk I/O
4. **Statistics**: Driver queries Ceph for real-time usage statistics

## Advanced Configuration

### Custom Ceph Configuration
```bash
# Example custom ceph.conf
[global]
mon_host = 10.0.1.10,10.0.1.11,10.0.1.12
auth_cluster_required = cephx
auth_service_required = cephx
auth_client_required = cephx

[client.admin]
keyring = /etc/ceph/ceph.client.admin.keyring
```

## Monitoring and Troubleshooting

### Log Files
- **SM Logs**: `/var/log/SMlog` - Storage Manager operations
- **XAPI Logs**: `/var/log/xensource.log` - API calls and errors
- **System Logs**: `/var/log/messages` - Kernel RBD mapping issues

### Common Issues

#### "VDI is still mapped on one or more hosts"
```bash
# Check mapped RBD devices
rbd showmapped

# Force unmap if necessary
rbd unmap /dev/rbd0
```

#### "Cannot connect to Ceph cluster"
```bash
# Test Ceph connectivity
ceph status
ceph health

# Check network connectivity to monitors
ping <monitor-ip>
telnet <monitor-ip> 6789
```

#### "Pool not found"
```bash
# List available pools
ceph osd pool ls

# Create pool if needed
ceph osd pool create rbd 64 64
ceph osd pool application enable rbd rbd
```

#### RBD Image Feature Compatibility
```bash
# Disable unsupported features for older kernels
rbd feature disable <image-name> object-map fast-diff deep-flatten
```

### Performance Monitoring
```bash
# Monitor Ceph cluster performance
ceph status
ceph df
ceph osd perf

# Monitor RBD performance
rbd perf image iostat

# Check pool statistics
ceph osd pool stats rbd
```

## Development

### Code Structure

#### CephRBDSR.py - Direct RBD Driver
```
CephRBDSR.py           # Direct RBD driver implementation
├── CephRBDSR          # SR (Storage Repository) class
│   ├── create()       # Create/attach SR
│   ├── delete()       # Delete/detach SR  
│   ├── scan()         # Discover existing VDIs
│   └── stat()         # Update capacity statistics
└── CephRBDVDI         # VDI (Virtual Disk Image) class
    ├── create()       # Create RBD image
    ├── delete()       # Delete RBD image
    ├── attach()       # Map RBD device
    ├── detach()       # Unmap RBD device
    ├── snapshot()     # Create snapshot/clone
    └── resize()       # Resize RBD image
```

#### LVMoRBDSR.py - LVHD over RBD Driver
```
LVMoRBDSR.py           # LVHD over RBD driver implementation  
├── LVMoRBDSR          # SR class (inherits from LVHDSR)
│   ├── create()       # Create RBD image and LVM VG
│   ├── delete()       # Delete LVM VG and RBD image
│   ├── attach()       # Map RBD and activate LVM
│   ├── detach()       # Deactivate LVM and unmap RBD
│   ├── scan()         # Scan LVM with RBD-aware config
│   ├── probe()        # Probe existing LVM on RBD
│   ├── _map_rbd_image()     # RBD mapping operations
│   ├── _unmap_rbd_image()   # RBD unmapping operations
│   ├── _with_rbd_lvm_conf() # LVM config override
│   ├── _deactivate_vg()     # VG deactivation
│   ├── _export_vg()         # VG export for cleanup
│   └── _parse_size_with_units() # Size parsing (M/G/T)
└── LVMoRBDVDI         # VDI class (inherits from LVHDVDI)
    ├── generate_config()      # VDI configuration
    └── attach_from_config()   # Config-based attach
```

#### Key Architectural Differences
- **CephRBDSR**: Each VDI is a separate RBD image, providing native Ceph features
- **LVMoRBDSR**: Single RBD image contains LVM volume group, VDIs are logical volumes
- **LVMoRBDSR** includes RBD device type support in LVM configuration (`/etc/lvm/lvmorbd/`)
- **LVMoRBDSR** uses `LVM_SYSTEM_DIR` environment variable for RBD-aware LVM operations

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Follow existing code style and patterns
- Test changes thoroughly in a lab environment
- Update documentation for new features
- Ensure compatibility with XCP-ng SMAPIv1 requirements

## License

This project is licensed under the GPL v2 License - see the [LICENSE](LICENSE) file for details.

## Support

### Community Support
- **GitHub Issues**: Report bugs and request features
- **XCP-ng Forum**: Community discussions
- **Ceph Community**: Ceph-specific questions

## Acknowledgments

- XCP-ng Project for the excellent hypervisor platform
- Ceph Community for the robust distributed storage system
- Original inspiration from other open-source SM drivers

---

**Note**: This driver is community-developed and not officially supported by Citrix or the XCP-ng project. Use in production at your own risk and ensure proper testing in your environment.
