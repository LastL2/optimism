#!/usr/bin/env bash
set -euo pipefail

DEPLOY_MNEMONIC="test test test test test test test test test test test junk"
MNEMONIC_PATH="m/44'/60'/0'/0/0"

echo "> Deploying contracts to generate state diff (non-broadcast)"
forge script -vvv scripts/Deploy.s.sol:Deploy --rpc-url "http://127.0.0.1:8545" --sig 'runWithStateDiff()' --mnemonic-passphrases "$DEPLOY_MNEMONIC" --mnemonic-derivation-paths "$MNEMONIC_PATH"
