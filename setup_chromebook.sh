#!/bin/bash

# #######################################################################################
# 
# INSTRUCTIONS:
# Step 1: Run the script
# Step 2: After script completes, close the terminal and reopen the terminal
#
# This script will install following packages on Chromebook:
# 
# gcloud skaffold kubectl kustomize kpt nomos anthoscli etc
# vs code terraform terraform-docs terraform-validator pre-commit  
# docker python3 jupyter-notebook
#
# #######################################################################################

# enable debugging
set -x

# abort on error
set -e

CLOUDSDK_INSTALL_DIR=$HOME/.local
TERRAFORM_VERSION=0.13.6
TERRAFORM_VALIDATOR_VERSION=2021-03-22
TERRAFORM_DOCS_VERSION=0.10.1

sudo apt-get update
mkdir -p $CLOUDSDK_INSTALL_DIR/bin

# vs code
wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > packages.microsoft.gpg
sudo install -o root -g root -m 644 packages.microsoft.gpg /etc/apt/trusted.gpg.d/
sudo sh -c 'echo "deb [arch=amd64,arm64,armhf signed-by=/etc/apt/trusted.gpg.d/packages.microsoft.gpg] https://packages.microsoft.com/repos/code stable main" > /etc/apt/sources.list.d/vscode.list'
sudo apt-get update
sudo apt-get install -y code
rm packages.microsoft.gpg

# gcloud sdk
if [[ -d $CLOUDSDK_INSTALL_DIR/google-cloud-sdk ]]; then
    echo "gcloud-sdk is already installed at $CLOUDSDK_INSTALL_DIR/google-cloud-sdk"
else
    curl https://sdk.cloud.google.com > install.sh
    chmod +x install.sh
    export CLOUDSDK_CORE_DISABLE_PROMPTS=1
    ./install.sh --install-dir=$CLOUDSDK_INSTALL_DIR
    $CLOUDSDK_INSTALL_DIR/google-cloud-sdk/bin/gcloud components install skaffold kubectl kustomize kpt nomos
    echo "export PATH=$PATH:$CLOUDSDK_INSTALL_DIR/google-cloud-sdk/bin" >> "$HOME/.bashrc" 
    echo ". $CLOUDSDK_INSTALL_DIR/google-cloud-sdk/path.bash.inc" >> "$HOME/.bashrc"
    echo ". $CLOUDSDK_INSTALL_DIR/google-cloud-sdk/completion.bash.inc" >> "$HOME/.bashrc"
    rm install.sh
fi

# terraform
sudo apt-get install -y unzip
curl -k -O https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip
unzip terraform_${TERRAFORM_VERSION}_linux_amd64.zip
sudo mv terraform /usr/local/bin/
rm terraform_${TERRAFORM_VERSION}_linux_amd64.zip

# terraform-docs
curl -k -LO https://github.com/terraform-docs/terraform-docs/releases/download/v${TERRAFORM_DOCS_VERSION}/terraform-docs-v${TERRAFORM_DOCS_VERSION}-linux-amd64
mv terraform-docs-v${TERRAFORM_DOCS_VERSION}-linux-amd64 terraform-docs
chmod +x terraform-docs
sudo mv terraform-docs /usr/local/bin

# python
sudo apt install -y python3 python3-dev python3-venv python3-pip 
pip3 install pip-tools

# jupyter notebook
pip3 install jupyterlab
pip3 install bash_kernel
python3 -m bash_kernel.install

# pre-commit terraform
pip3 install pre-commit

# tflint
curl https://raw.githubusercontent.com/terraform-linters/tflint/master/install_linux.sh | bash

# terraform-validator
$CLOUDSDK_INSTALL_DIR/google-cloud-sdk/bin/gsutil cp gs://terraform-validator/releases/${TERRAFORM_VALIDATOR_VERSION}/terraform-validator-linux-amd64 .
chmod +x terraform-validator-linux-amd64
sudo mv terraform-validator-linux-amd64 /usr/local/bin/terraform-validator

# docker
sudo apt-get install -y apt-transport-https ca-certificates gnupg lsb-release
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io
sudo usermod -a -G docker ${USER}
