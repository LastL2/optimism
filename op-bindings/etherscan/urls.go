package etherscan

import "fmt"

func (c *client) constructUrl(chain, action, address, module, params string) (string, error) {
	var baseUrl string
	switch chain {
	case "eth":
		baseUrl = c.baseUrlEth
	case "op":
		baseUrl = c.baseUrlOp
	default:
		return "", fmt.Errorf("unknown chain: %s", chain)
	}

	queryFragment := fmt.Sprintf("?module=%s&action=%s&%s", module, action, params)
	return fmt.Sprintf(baseUrl, queryFragment), nil
}

func (c *client) getAbiUrl(chain, contractAddress string) (string, error) {
	return c.constructUrl(chain, "getabi", contractAddress, "contract", fmt.Sprintf("address=%s", contractAddress))
}

func (c *client) getDeploymentTxHashUrl(chain, contractAddress string) (string, error) {
	return c.constructUrl(chain, "getcontractcreation", contractAddress, "contract", fmt.Sprintf("contractaddresses=%s", contractAddress))
}

func (c *client) getDeployedBytecodeUrl(chain, contractAddress string) (string, error) {
	return c.constructUrl(chain, "eth_getCode", contractAddress, "proxy", fmt.Sprintf("address=%s", contractAddress))
}

func (c *client) getTxByHashUrl(chain, txHash string) (string, error) {
	return c.constructUrl(chain, "eth_getTransactionByHash", txHash, "proxy", fmt.Sprintf("txHash=%s&tag=latest", txHash))
}
