package main

import (
	"fmt"
	"os"

	"github.com/ethereum-optimism/optimism/op-bindings/etherscan"
	oplog "github.com/ethereum-optimism/optimism/op-service/log"
	"github.com/ethereum/go-ethereum/ethclient"
	gethLog "github.com/ethereum/go-ethereum/log"
	"github.com/urfave/cli/v2"
)

type bindGenGeneratorBase struct {
	metadataOut         string
	bindingsPackageName string
	monorepoBasePath    string
	contractsListPath   string
	logger              gethLog.Logger
}

const (
	// Base Flags
	MetadataOutFlagName         = "metadata-out"
	BindingsPackageNameFlagName = "bindings-package"
	MonoRepoBaseFlagName        = "monorepo-base"
	ContractsListFlagName       = "contracts-list"
	LogLevelFlagName            = "log.level"

	// Local Contracts Flags
	SourceMapsListFlagName = "source-maps-list"
	ForgeArtifactsFlagName = "forge-artifacts"

	// Remote Contracts Flags
	EtherscanApiKeyEthFlagName = "etherscan.apikey.eth"
	EtherscanApiKeyOpFlagName  = "etherscan.apikey.op"
	RpcUrlEthFlagName          = "rpc.url.eth"
	RpcUrlOpFlagName           = "rpc.url.op"
)

func main() {
	oplog.SetupDefaults()

	app := &cli.App{
		Name:  "BindGen",
		Usage: "Generate contract bindings using Foundry artifacts and/or remotely sourced contract data",
		Commands: []*cli.Command{
			{
				Name:  "generate",
				Usage: "Generate contract bindings",
				Flags: baseFlags(),
				Subcommands: []*cli.Command{
					{
						Name:   "all",
						Usage:  "Generate bindings for local and remote contracts",
						Flags:  append(localFlags(), remoteFlags()...),
						Action: generateBindings,
					},
					{
						Name:   "local",
						Usage:  "Generate bindings for locally sourced contracts",
						Flags:  localFlags(),
						Action: generateBindings,
					},
					{
						Name:   "remote",
						Usage:  "Generate bindings for remotely sourced contracts",
						Flags:  remoteFlags(),
						Action: generateBindings,
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		gethLog.Crit("Error staring CLI app", "error", err.Error())
	}
}

func setupLogger(c *cli.Context) (gethLog.Logger, error) {
	logger := oplog.NewLogger(oplog.AppOut(c), oplog.ReadCLIConfig(c))
	oplog.SetGlobalLogHandler(logger.GetHandler())
	return logger, nil
}

func generateBindings(c *cli.Context) error {
	logger, _ := setupLogger(c)

	var err error
	switch c.Command.Name {
	case "all":
		localBindingsGenerator := parseConfigLocal(logger, c)
		if err := localBindingsGenerator.generateBindings(); err != nil {
			gethLog.Crit("Error generating local bindings", "error", err.Error())
		}

		var remoteBindingsGenerator bindGenGeneratorRemote
		if remoteBindingsGenerator, err = parseConfigRemote(logger, c); err != nil {
			gethLog.Crit("Error parsing remote config", "error", err.Error())
		}
		if err := remoteBindingsGenerator.generateBindings(); err != nil {
			gethLog.Crit("Error generating remote bindings", "error", err.Error())
		}

		return nil
	case "local":
		localBindingsGenerator := parseConfigLocal(logger, c)
		if err := localBindingsGenerator.generateBindings(); err != nil {
			gethLog.Crit("Error generating local bindings", "error", err.Error())
		}
		return nil
	case "remote":
		var remoteBindingsGenerator bindGenGeneratorRemote
		if remoteBindingsGenerator, err = parseConfigRemote(logger, c); err != nil {
			gethLog.Crit("Error parsing remote config", "error", err.Error())
		}
		if err := remoteBindingsGenerator.generateBindings(); err != nil {
			gethLog.Crit("Error generating remote bindings", "error", err.Error())
		}
		return nil
	default:
		return fmt.Errorf("unknown command: %s", c.Command.Name)
	}
}

func parseConfigBase(logger gethLog.Logger, c *cli.Context) bindGenGeneratorBase {
	return bindGenGeneratorBase{
		metadataOut:         c.String(MetadataOutFlagName),
		bindingsPackageName: c.String(BindingsPackageNameFlagName),
		monorepoBasePath:    c.String(MonoRepoBaseFlagName),
		contractsListPath:   c.String(ContractsListFlagName),
		logger:              logger,
	}
}

func parseConfigLocal(logger gethLog.Logger, c *cli.Context) bindGenGeneratorLocal {
	baseConfig := parseConfigBase(logger, c)
	return bindGenGeneratorLocal{
		bindGenGeneratorBase:  baseConfig,
		sourceMapsList:     c.String(SourceMapsListFlagName),
		forgeArtifactsPath: c.String(ForgeArtifactsFlagName),
	}
}

func parseConfigRemote(logger gethLog.Logger, c *cli.Context) (bindGenGeneratorRemote, error) {
	baseConfig := parseConfigBase(logger, c)
	generator := bindGenGeneratorRemote{
		bindGenGeneratorBase:  baseConfig,
		etherscanApiKeyEth: c.String(EtherscanApiKeyEthFlagName),
		etherscanApiKeyOp:  c.String(EtherscanApiKeyOpFlagName),
	}
	generator.contractDataClient = etherscan.NewClient(generator.etherscanApiKeyEth, generator.etherscanApiKeyOp)

	var err error
	if generator.rpcClientEth, err = ethclient.Dial(c.String(RpcUrlEthFlagName)); err != nil {
		return bindGenGeneratorRemote{}, fmt.Errorf("error initializing Ethereum client: %s", err)
	}
	if generator.rpcClientOp, err = ethclient.Dial(c.String(RpcUrlOpFlagName)); err != nil {
		return bindGenGeneratorRemote{}, fmt.Errorf("error initializing Optimism client: %s", err)
	}
	return generator, nil
}

func baseFlags() []cli.Flag {
	baseFlags := []cli.Flag{
		&cli.StringFlag{
			Name:     MetadataOutFlagName,
			Usage:    "Output directory to put contract metadata files in",
			Required: true,
		},
		&cli.StringFlag{
			Name:     BindingsPackageNameFlagName,
			Usage:    "Go package name given to generated bindings",
			Required: true,
		},
		&cli.StringFlag{
			Name:     MonoRepoBaseFlagName,
			Usage:    "Path to the base of the monorepo",
			Required: true,
		},
		&cli.StringFlag{
			Name:     ContractsListFlagName,
			Usage:    "Path to file containing list of contract names to generate bindings for",
			Required: true,
		},
	}

	return append(baseFlags, oplog.CLIFlags("bindgen")...)
}

func localFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  SourceMapsListFlagName,
			Usage: "Comma-separated list of contracts to generate source-maps for",
		},
		&cli.StringFlag{
			Name:     ForgeArtifactsFlagName,
			Usage:    "Path to forge-artifacts directory, containing compiled contract artifacts",
			Required: true,
		},
	}
}

func remoteFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  EtherscanApiKeyEthFlagName,
			Usage: "API key to make queries to Etherscan for Ethereum",
			Required: true,
		},
		&cli.StringFlag{
			Name:  EtherscanApiKeyOpFlagName,
			Usage: "API key to make queries to Etherscan for Optimism",
			Required: true,
		},
		&cli.StringFlag{
			Name:  RpcUrlEthFlagName,
			Usage: "RPC URL (with API key if required) to query Ethereum",
			Required: true,
		},
		&cli.StringFlag{
			Name:  RpcUrlOpFlagName,
			Usage: "RPC URL (with API key if required) to query Optimism",
			Required: true,
		},
	}
}
