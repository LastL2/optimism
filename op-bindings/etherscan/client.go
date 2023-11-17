package etherscan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/retry"
)

type client struct {
	baseUrlEth string
	baseUrlOp  string
}

type apiResponse struct {
	Status  string      `json:"status"`
	Message string      `json:"message"`
	Result  interface{} `json:"result"`
}

type rpcResponse struct {
	JsonRpc string      `json:"jsonrpc"`
	Id      int         `json:"id"`
	Result  interface{} `json:"result"`
}

type TxInfo struct {
	To string `json:"to"`
	Input string `json:"input"`
}

const apiMaxRetries = 3
const apiRetryDelay = time.Duration(2) * time.Second

func NewClient(apiKeyEth, apiKeyOp string) *client {
	return &client{
		baseUrlEth: "https://api.etherscan.io/api/%s&apikey=" + apiKeyEth,
		baseUrlOp:  "https://api-optimistic.etherscan.io/api/%s&apikey=" + apiKeyOp,
	}
}

func (c *client) fetch(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (c *client) fetchEtherscanApi(url string) (apiResponse, error) {
	return retry.Do[apiResponse](context.Background(), apiMaxRetries, retry.Fixed(apiRetryDelay), func() (apiResponse, error) {
		body, err := c.fetch(url)
		if err != nil {
			return apiResponse{}, err
		}

		var response apiResponse
		err = json.Unmarshal(body, &response)
		if err != nil {
			return apiResponse{}, fmt.Errorf("failed to unmarshal as apiResponse: %w", err)
		}

		if response.Message != "OK" {
			if response.Result == "Max rate limit reached" {
				return apiResponse{}, errors.New("max rate limit reached")
			}

			return apiResponse{}, fmt.Errorf("there was an issue with the Etherscan request to %s, received response: %v", url, response)
		}

		return response, nil
	})
}

func (c *client) fetchEtherscanRpc(url string) (rpcResponse, error) {
	return retry.Do[rpcResponse](context.Background(), apiMaxRetries, retry.Fixed(apiRetryDelay), func() (rpcResponse, error) {
		body, err := c.fetch(url)
		if err != nil {
			return rpcResponse{}, err
		}

		var response rpcResponse
		err = json.Unmarshal(body, &response)
		if err != nil {
			return rpcResponse{}, fmt.Errorf("failed to unmarshal as rpcResponse: %w", err)
		}
		if err == nil && response.Result != "Max rate limit reached" {
			return response, nil
		}

		// Etherscan's response will be of struct rpcResponse if successful,
		// however a rate limit error will return apiResponse
		var responseApi apiResponse
		err = json.Unmarshal(body, &responseApi)
		if err != nil {
			return rpcResponse{}, fmt.Errorf("failed to unmarshal as rpcResponse: %w", err)
		}

		if responseApi.Result == "Max rate limit reached" {
			return rpcResponse{}, errors.New("max rate limit reached")
		}

		return rpcResponse{}, fmt.Errorf("there was an issue with the Etherscan request to %s, received response: %v", url, responseApi)
	})
}

func (c *client) FetchAbi(chain, address string) (string, error) {
	url, err := c.getAbiUrl(chain, address)
	if err != nil {
		return "", err
	}

	response, err := c.fetchEtherscanApi(url)
	if err != nil {
		return "", err
	}

	abi, ok := response.Result.(string)
	if !ok {
		return "", fmt.Errorf("API response result is not expected ABI string")
	}

	return abi, nil
}

func (c *client) FetchDeployedBytecode(chain, address string) (string, error) {
	url, err := c.getDeployedBytecodeUrl(chain, address)
	if err != nil {
		return "", err
	}

	response, err := c.fetchEtherscanRpc(url)
	if err != nil {
		return "", fmt.Errorf("error fetching deployed bytecode: %w", err)
	}

	bytecode, ok := response.Result.(string)
	if !ok {
		return "", errors.New("API response result is not expected bytecode string")
	}

	return bytecode, nil
}

func (c *client) FetchDeploymentTxHash(chain, address string) (string, error) {
	url, err := c.getDeploymentTxHashUrl(chain, address)
	if err != nil {
		return "", err
	}

	response, err := c.fetchEtherscanApi(url)
	if err != nil {
		return "", err
	}

	results, ok := response.Result.([]interface{})
	if !ok {
		return "", fmt.Errorf("failed to assert API response: %s is type of []txInfo", response)
	}
	if len(results) == 0 {
		return "", fmt.Errorf("API response result is an empty array")
	}

	deploymentTxInfo, ok := results[0].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("failed to assert API response result[0] is type of txInfo")
	}

	txHash, ok := deploymentTxInfo["txHash"].(string)
	if !ok {
		return "", fmt.Errorf("failed to assert API response result[0][\"txHash\"] is type of string")
	}

	return txHash, nil
}

func (c *client) FetchDeploymentTx(chain, txHash string) (TxInfo, error) {
	url, err := c.getTxByHashUrl(chain, txHash)
	if err != nil {
		return TxInfo{}, err
	}

	response, err := c.fetchEtherscanRpc(url)
	if err != nil {
		return TxInfo{}, err
	}

	resultBytes, err := json.Marshal(response.Result)
	if err != nil {
		return TxInfo{}, fmt.Errorf("failed to marshal Result into JSON: %w", err)
	}

	var tx TxInfo
	err = json.Unmarshal(resultBytes, &tx)
	if err != nil {
		return TxInfo{}, fmt.Errorf("API response result is not expected txInfo struct: %w", err)
	}

	return tx, nil
}
