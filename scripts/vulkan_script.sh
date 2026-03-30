#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "================================================="
echo " Full Vulkan Setup for NVIDIA L4 (Colab Headless)"
echo "================================================="

echo "[1/5] Updating package lists..."
sudo apt-get update -y -q

echo "[2/5] Detecting NVIDIA Driver Version..."
# Extract the major driver version (e.g., 535) from nvidia-smi
DRIVER_VER=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader | cut -d'.' -f1 | head -n 1)
echo "      -> Detected Driver Version: $DRIVER_VER"

echo "[3/5] Installing Vulkan utilities and missing NVIDIA GL libraries..."
# Install the loader, tools, and the GL package containing the actual Vulkan implementation
# DEBIAN_FRONTEND=noninteractive prevents apt from popping up graphical configuration prompts
DEBIAN_FRONTEND=noninteractive sudo apt-get install -y -q libvulkan1 vulkan-tools libnvidia-gl-${DRIVER_VER}

echo "[4/5] Creating the NVIDIA ICD JSON routing file..."
sudo mkdir -p /etc/vulkan/icd.d/
sudo mkdir -p /usr/share/vulkan/icd.d/

# Write the JSON manifest
cat <<EOF | sudo tee /etc/vulkan/icd.d/nvidia_icd.json > /dev/null
{
    "file_format_version" : "1.0.0",
    "ICD": {
        "library_path": "libGLX_nvidia.so.0",
        "api_version" : "1.3.0"
    }
}
EOF

# Create a symlink in the secondary loader directory just to be safe
sudo ln -sf /etc/vulkan/icd.d/nvidia_icd.json /usr/share/vulkan/icd.d/nvidia_icd.json

echo "[5/5] Setting Environment Variables..."
export VK_ICD_FILENAMES=/etc/vulkan/icd.d/nvidia_icd.json

echo "================================================="
echo " Setup Complete! Verifying GPU communication...  "
echo "================================================="

# Verify the setup
vulkaninfo | grep -i "deviceName" || echo "Warning: Could not detect device. Please check outputs above."

echo "Done! Vulkan is ready."