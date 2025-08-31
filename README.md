# CephRBDSR - Ceph RBD Storage Repository Driver for XCP-ng

A production-ready SMAPIv2 storage driver that enables XCP-ng to use Ceph RADOS Block Device (RBD) images as virtual disk storage, providing distributed, scalable, and highly available block storage.

## Features

- **SMAPIv2 Compliance**: Full integration with XCP-ng's Storage Manager framework
- **Thin Provisioning**: Efficient space utilization with Ceph's thin provisioning
- **High Availability**: Leverages Ceph's distributed architecture and replication
- **Live Operations**: Support for online resize, snapshots, and cloning
- **Pool Quota Support**: Respects Ceph pool quotas when configured
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
- **Ceph version**: Nautilus or later (tested with Nautilus/Octopus)
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
# Install Ceph repository
wget -q -O- 'https://download.ceph.com/keys/release.asc' | apt-key add -
echo deb https://download.ceph.com/debian-nautilus/ $(lsb_release -sc) main | tee /etc/apt/sources.list.d/ceph.list
apt-get update

# Install Ceph client
apt-get install ceph-common
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
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   XCP-ng Host   │    │   Ceph Cluster   │    │   RBD Images    │
│                 │    │                  │    │                 │
│  ┌──────────────┤    │  ┌─────────────┐ │    │ ┌─────────────┐ │
│  │ XAPI/SMAPI   │    │  │   Monitor   │ │    │ │ vdi-uuid-1  │ │
│  └──────────────┤    │  └─────────────┘ │    │ └─────────────┘ │
│  ┌──────────────┤    │  ┌─────────────┐ │    │ ┌─────────────┐ │
│  │ CephRBDSR.py │◄──►│  │     OSD     │ │◄──►│ │ vdi-uuid-2  │ │
│  └──────────────┤    │  └─────────────┘ │    │ └─────────────┘ │
│  ┌──────────────┤    │  ┌─────────────┐ │    │ ┌─────────────┐ │
│  │   /dev/rbd0  │    │  │     OSD     │ │    │ │ vdi-uuid-n  │ │
│  └──────────────┘    │  └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └──────────────────┘    └─────────────────┘
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

### Performance Tuning
```bash
# Optimize RBD for virtualization workloads
ceph osd pool set rbd size 3
ceph osd pool set rbd min_size 2
ceph osd pool set rbd crush_rule replicated_rule

# Enable RBD exclusive locks and object map (if supported)
ceph osd pool application enable rbd rbd
```

### Multi-Pool Setup
Create multiple SRs for different storage tiers:

```bash
# SSD pool for high-performance VMs
xe sr-create type=cephrbd name-label="Ceph SSD" device-config:pool=ssd-pool

# HDD pool for bulk storage
xe sr-create type=cephrbd name-label="Ceph HDD" device-config:pool=hdd-pool
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

### Testing
The repository includes test scripts for development:

- `test_cephrbd.py` - Basic functionality tests
- `test_quota.py` - Pool quota functionality tests  
- `test_stats.py` - Statistics and capacity tests
- `test_vdi_creation.py` - VDI lifecycle tests

```bash
# Run basic tests (requires Ceph access)
python test_cephrbd.py
```

### Code Structure
```
CephRBDSR.py           # Main driver implementation
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
- Ensure compatibility with XCP-ng SMAPIv2 requirements

## License

This project is licensed under the GPL v2 License - see the [LICENSE](LICENSE) file for details.

## Support

### Community Support
- **GitHub Issues**: Report bugs and request features
- **XCP-ng Forum**: Community discussions
- **Ceph Community**: Ceph-specific questions

### Commercial Support
For production deployments requiring commercial support, consider:
- XCP-ng Pro support
- Ceph Enterprise support
- Third-party integration specialists

## Acknowledgments

- XCP-ng Project for the excellent hypervisor platform
- Ceph Community for the robust distributed storage system
- Original inspiration from other open-source SM drivers

## Changelog

### v1.0.0 (2024-08-31)
- Initial release with full SMAPIv2 support
- Pool quota support
- XCP-ng kernel compatibility optimizations
- Comprehensive error handling and recovery
- Production-ready stability improvements

---

**Note**: This driver is community-developed and not officially supported by Citrix or the XCP-ng project. Use in production at your own risk and ensure proper testing in your environment.