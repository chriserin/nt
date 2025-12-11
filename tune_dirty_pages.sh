#!/bin/bash
# tune_dirty_pages.sh - Configure VM dirty page settings for high-throughput writes
#
# Usage:
#   sudo ./tune_dirty_pages.sh set     # Configure for high-performance writes
#   sudo ./tune_dirty_pages.sh reset   # Restore to default settings

set -e

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Error: This script must be run as root (use sudo)"
    exit 1
fi

# Default values (typical Linux defaults)
DEFAULT_DIRTY_RATIO=20
DEFAULT_DIRTY_BACKGROUND_RATIO=10
DEFAULT_DIRTY_WRITEBACK_CENTISECS=500
DEFAULT_DIRTY_EXPIRE_CENTISECS=3000

# High-performance values for 64 GB RAM system
# 32 GB max dirty, 16 GB background flush
HP_DIRTY_BYTES=$((32 * 1024 * 1024 * 1024))
HP_DIRTY_BACKGROUND_BYTES=$((16 * 1024 * 1024 * 1024))
HP_DIRTY_WRITEBACK_CENTISECS=100 # Flush every 1 second
HP_DIRTY_EXPIRE_CENTISECS=1000   # Flush pages older than 10 seconds

show_current() {
    echo "Current settings:"
    echo "  dirty_ratio:            $(cat /proc/sys/vm/dirty_ratio)%"
    echo "  dirty_background_ratio: $(cat /proc/sys/vm/dirty_background_ratio)%"
    echo "  dirty_bytes:            $(cat /proc/sys/vm/dirty_bytes) bytes"
    echo "  dirty_background_bytes: $(cat /proc/sys/vm/dirty_background_bytes) bytes"
    echo "  dirty_writeback_centisecs: $(cat /proc/sys/vm/dirty_writeback_centisecs) ($(($(cat /proc/sys/vm/dirty_writeback_centisecs) / 100))s)"
    echo "  dirty_expire_centisecs:    $(cat /proc/sys/vm/dirty_expire_centisecs) ($(($(cat /proc/sys/vm/dirty_expire_centisecs) / 100))s)"
}

set_high_performance() {
    echo "Setting high-performance dirty page parameters..."

    # Clear ratio-based settings (setting bytes overrides ratio, but clear anyway)
    sysctl -w vm.dirty_ratio=0 >/dev/null
    sysctl -w vm.dirty_background_ratio=0 >/dev/null

    # Set absolute byte limits
    sysctl -w vm.dirty_bytes=${HP_DIRTY_BYTES} >/dev/null
    sysctl -w vm.dirty_background_bytes=${HP_DIRTY_BACKGROUND_BYTES} >/dev/null

    # Set writeback timing
    sysctl -w vm.dirty_writeback_centisecs=${HP_DIRTY_WRITEBACK_CENTISECS} >/dev/null
    sysctl -w vm.dirty_expire_centisecs=${HP_DIRTY_EXPIRE_CENTISECS} >/dev/null

    echo "✓ High-performance settings applied:"
    echo "  Max dirty:        $((HP_DIRTY_BYTES / 1024 / 1024 / 1024)) GB"
    echo "  Background flush: $((HP_DIRTY_BACKGROUND_BYTES / 1024 / 1024 / 1024)) GB"
    echo "  Writeback every:  $((HP_DIRTY_WRITEBACK_CENTISECS / 100)) seconds"
    echo "  Expire after:     $((HP_DIRTY_EXPIRE_CENTISECS / 100)) seconds"
}

reset_defaults() {
    echo "Restoring default dirty page parameters..."

    # Restore ratio-based settings
    sysctl -w vm.dirty_ratio=${DEFAULT_DIRTY_RATIO} >/dev/null
    sysctl -w vm.dirty_background_ratio=${DEFAULT_DIRTY_BACKGROUND_RATIO} >/dev/null

    # Setting the ratios will override byte-based settings, so no need to explicitly clear them
    # Clear byte-based settings
    # sysctl -w vm.dirty_bytes=0 >/dev/null
    # sysctl -w vm.dirty_background_bytes=0 >/dev/null

    # Restore writeback timing
    sysctl -w vm.dirty_writeback_centisecs=${DEFAULT_DIRTY_WRITEBACK_CENTISECS} >/dev/null
    sysctl -w vm.dirty_expire_centisecs=${DEFAULT_DIRTY_EXPIRE_CENTISECS} >/dev/null

    echo "✓ Default settings restored:"
    echo "  dirty_ratio:            ${DEFAULT_DIRTY_RATIO}%"
    echo "  dirty_background_ratio: ${DEFAULT_DIRTY_BACKGROUND_RATIO}%"
    echo "  Writeback every:        $((DEFAULT_DIRTY_WRITEBACK_CENTISECS / 100)) seconds"
    echo "  Expire after:           $((DEFAULT_DIRTY_EXPIRE_CENTISECS / 100)) seconds"
}

# Main
case "${1:-}" in
set)
    set_high_performance
    echo ""
    show_current
    ;;
reset)
    reset_defaults
    echo ""
    show_current
    ;;
show)
    show_current
    ;;
*)
    echo "Usage: $0 {set|reset|show}"
    echo ""
    echo "Commands:"
    echo "  set   - Configure for high-performance writes (32 GB dirty buffer)"
    echo "  reset - Restore default settings"
    echo "  show  - Display current settings"
    echo ""
    echo "Note: This script requires root privileges (use sudo)"
    exit 1
    ;;
esac
