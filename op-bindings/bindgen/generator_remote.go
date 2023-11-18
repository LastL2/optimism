package main

import (
	"fmt"
	"os"

	"github.com/ethereum-optimism/optimism/op-bindings/etherscan"
)

type bindGenGeneratorRemote struct {
	bindGenGeneratorBase
	etherscanApiKeyEth string
	etherscanApiKeyOp  string
	contractDataClient
	rpcUrlEth        string
	rpcUrlOp         string
	tempArtifactsDir string
}

type contractDataClient interface {
	FetchAbi(chain, address string) (string, error)
	FetchDeployedBytecode(chain, address string) (string, error)
	FetchDeploymentTxHash(chain, address string) (string, error)
	FetchDeploymentTx(chain, txHash string) (etherscan.TxInfo, error)
}

type remoteContract struct {
	Name            string            `json:"name"`
	Verified        bool              `json:"verified"`
	Deployments     map[string]string `json:"deployments"`
	DeploymentSalt  string            `json:"deploymentSalt"`
	DeployerAddress string            `json:"deployerAddress"`
	Abi             string            `json:"abi"`
	InitBytecode    string            `json:"initBytecode"`
}

type remoteContractMetadata struct {
	Package     string
	InitBin     string
	DeployedBin string
	remoteContract
}

func (generator *bindGenGeneratorRemote) readContractsList() ([]remoteContract, error) {
	var list contractsList
	err := readJSONFile(generator.logger, generator.contractsListPath, &list)
	return list.Remote, err
}

func (generator *bindGenGeneratorRemote) generateBindings() error {
	contracts, err := generator.readContractsList()
	if err != nil {
		return fmt.Errorf("error reading contract list %s: %w", generator.contractsListPath, err)
	}
	if len(contracts) == 0 {
		return fmt.Errorf("no contracts parsed from given contract list: %s", generator.contractsListPath)
	}

	return generator.processContracts(contracts)
}

func (generator *bindGenGeneratorRemote) processContracts(contracts []remoteContract) error {
	var err error
	generator.tempArtifactsDir, err = mkTempArtifactsDir(generator.logger)
	if err != nil {
		return err
	}
	defer func() {
		err := os.RemoveAll(generator.tempArtifactsDir)
		if err != nil {
			generator.logger.Error("Error removing temporary artifact directory", "tempArtifactsDir", generator.tempArtifactsDir, "err", err.Error())
		} else {
			generator.logger.Debug("Successfully removed temporary artifact directory")
		}
	}()

	for _, contract := range contracts {
		generator.logger.Info("Generating bindings and metadata for remote contract", "contractName", contract.Name)

		contractMetadata := remoteContractMetadata{
			remoteContract: remoteContract{
				Name:           contract.Name,
				Deployments:    contract.Deployments,
				DeploymentSalt: contract.DeploymentSalt,
				Abi:            contract.Abi,
				Verified:       contract.Verified,
			},
			Package: generator.bindingsPackageName,
		}

		var err error
		switch contract.Name {
		case "MultiCall3", "Create2Deployer", "Safe_v130", "SafeL2", "MultiSendCallOnly",
			"EntryPoint", "SafeSingletonFactory", "DeterministicDeploymentProxy":
			err = generator.standardHandler(&contractMetadata)
		case "MultiSend":
			err = generator.multiSendHandler(&contractMetadata)
		case "SenderCreator":
			// The SenderCreator contract is deployed by EntryPoint, so the transaction data
			// from the deployment transaction is for the entire EntryPoint deployment.
			// So, we're manually providing the initialization bytecode
			contractMetadata.InitBin = contract.InitBytecode
			err = generator.senderCreatorHandler(&contractMetadata)
		case "Permit2":
			// Permit2 has an immutable Solidity variable that resolves to block.chainid,
			// so we can't use the deployed bytecode, and instead must generate it
			// at some later point not handled by BindGen.
			// DeployerAddress is intended to be used to help deploy Permit2 at it's deterministic address
			// to a chain set with the required id to be able to obtain a diff minimized deployed bytecode
			contractMetadata.DeployerAddress = contract.DeployerAddress
			err = generator.permit2Handler(&contractMetadata)
		default:
			err = fmt.Errorf("unknown contract: %s, don't know how to handle it", contract.Name)
		}

		if err != nil {
			return err
		}
	}

	return nil
}
