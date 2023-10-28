//! This module contains the logic for reading a block's fully hydrated receipts directly from the
//! [reth] database.

use anyhow::{anyhow, Result};
use reth_blockchain_tree::noop::NoopBlockchainTree;
use reth_db::open_db_read_only;
use reth_primitives::{
    BlockHashOrNumber, Receipt, TransactionKind, TransactionMeta, TransactionSigned, MAINNET, U128,
    U256, U64,
};
use reth_provider::{providers::BlockchainProvider, BlockReader, ProviderFactory, ReceiptProvider};
use reth_rpc_types::{Log, TransactionReceipt};
use std::{ffi::c_char, path::Path};

/// A [ReceiptsResult] is a wrapper around a JSON string containing serialized [TransactionReceipt]s
/// as well as an error status that is compatible with FFI.
///
/// # Safety
/// - When the `error` field is false, the `data` pointer is guaranteed to be valid.
/// - When the `error` field is true, the `data` pointer is guaranteed to be null.
#[repr(C)]
pub struct ReceiptsResult {
    data: *mut char,
    data_len: usize,
    error: bool,
}

impl ReceiptsResult {
    /// Constructs a successful [ReceiptsResult] from a JSON string.
    pub fn success(data: *mut char, data_len: usize) -> Self {
        Self {
            data,
            data_len,
            error: false,
        }
    }

    /// Constructs a failing [ReceiptsResult] with a null pointer to the data.
    pub fn fail() -> Self {
        Self {
            data: std::ptr::null_mut(),
            data_len: 0,
            error: true,
        }
    }
}

/// Read the receipts for a blockhash from the RETH database directly.
///
/// # Safety
/// - All possible nil pointer dereferences are checked, and the function will return a
///   failing [ReceiptsResult] if any are found.
#[inline(always)]
pub(crate) unsafe fn read_receipts_inner(
    block_hash: *const u8,
    block_hash_len: usize,
    db_path: *const c_char,
) -> Result<ReceiptsResult> {
    // Convert the raw pointer and length back to a Rust slice
    let block_hash: [u8; 32] = {
        if block_hash.is_null() {
            anyhow::bail!("block_hash pointer is null");
        }
        std::slice::from_raw_parts(block_hash, block_hash_len)
    }
    .try_into()?;

    // Convert the *const c_char to a Rust &str
    let db_path_str = {
        if db_path.is_null() {
            anyhow::bail!("db path pointer is null");
        }
        std::ffi::CStr::from_ptr(db_path)
    }
    .to_str()?;

    let db = open_db_read_only(Path::new(db_path_str), None).map_err(|e| anyhow!(e))?;
    let factory = ProviderFactory::new(db, MAINNET.clone());

    // Create a read-only BlockChainProvider
    let provider = BlockchainProvider::new(factory, NoopBlockchainTree::default())?;

    // Fetch the block and the receipts within it
    let block = provider
        .block_by_hash(block_hash.into())?
        .ok_or(anyhow!("Failed to fetch block"))?;
    let receipts = provider
        .receipts_by_block(BlockHashOrNumber::Hash(block_hash.into()))?
        .ok_or(anyhow!("Failed to fetch block receipts"))?;

    let block_number = block.number;
    let base_fee = block.base_fee_per_gas;
    let block_hash = block.hash_slow();
    let receipts = block
        .body
        .into_iter()
        .zip(receipts.clone())
        .enumerate()
        .map(|(idx, (tx, receipt))| {
            let meta = TransactionMeta {
                tx_hash: tx.hash,
                index: idx as u64,
                block_hash,
                block_number,
                base_fee,
                excess_blob_gas: None,
            };
            build_transaction_receipt_with_block_receipts(tx, meta, receipt, &receipts)
        })
        .collect::<Option<Vec<_>>>()
        .ok_or(anyhow!("Failed to build receipts"))?;

    // Convert the receipts to JSON for transport
    let mut receipts_json = serde_json::to_string(&receipts)?;

    // Create a ReceiptsResult with a pointer to the json-ified receipts
    let res = ReceiptsResult::success(receipts_json.as_mut_ptr() as *mut char, receipts_json.len());

    // Forget the `receipts_json` string so that its memory isn't freed by the
    // borrow checker at the end of this scope
    std::mem::forget(receipts_json); // Prevent Rust from freeing the memory

    Ok(res)
}

/// Builds a hydrated [TransactionReceipt] from information in the passed transaction,
/// receipt, and block receipts.
///
/// Returns [None] if the transaction's sender could not be recovered from the signature.
#[inline(always)]
fn build_transaction_receipt_with_block_receipts(
    tx: TransactionSigned,
    meta: TransactionMeta,
    receipt: Receipt,
    all_receipts: &[Receipt],
) -> Option<TransactionReceipt> {
    let transaction = tx.clone().into_ecrecovered()?;

    // get the previous transaction cumulative gas used
    let gas_used = if meta.index == 0 {
        receipt.cumulative_gas_used
    } else {
        let prev_tx_idx = (meta.index - 1) as usize;
        all_receipts
            .get(prev_tx_idx)
            .map(|prev_receipt| receipt.cumulative_gas_used - prev_receipt.cumulative_gas_used)
            .unwrap_or_default()
    };

    let mut res_receipt = TransactionReceipt {
        transaction_hash: Some(meta.tx_hash),
        transaction_index: U64::from(meta.index),
        block_hash: Some(meta.block_hash),
        block_number: Some(U256::from(meta.block_number)),
        from: transaction.signer(),
        to: None,
        cumulative_gas_used: U256::from(receipt.cumulative_gas_used),
        gas_used: Some(U256::from(gas_used)),
        contract_address: None,
        logs: Vec::with_capacity(receipt.logs.len()),
        effective_gas_price: U128::from(transaction.effective_gas_price(meta.base_fee)),
        transaction_type: tx.transaction.tx_type().into(),
        // TODO pre-byzantium receipts have a post-transaction state root
        state_root: None,
        logs_bloom: receipt.bloom_slow(),
        status_code: if receipt.success {
            Some(U64::from(1))
        } else {
            Some(U64::from(0))
        },

        // EIP-4844 fields
        blob_gas_price: None,
        blob_gas_used: None,
    };

    match tx.transaction.kind() {
        TransactionKind::Create => {
            res_receipt.contract_address =
                Some(transaction.signer().create(tx.transaction.nonce()));
        }
        TransactionKind::Call(addr) => {
            res_receipt.to = Some(*addr);
        }
    }

    // get number of logs in the block
    let mut num_logs = 0;
    for prev_receipt in all_receipts.iter().take(meta.index as usize) {
        num_logs += prev_receipt.logs.len();
    }

    for (tx_log_idx, log) in receipt.logs.into_iter().enumerate() {
        let rpclog = Log {
            address: log.address,
            topics: log.topics,
            data: log.data,
            block_hash: Some(meta.block_hash),
            block_number: Some(U256::from(meta.block_number)),
            transaction_hash: Some(meta.tx_hash),
            transaction_index: Some(U256::from(meta.index)),
            log_index: Some(U256::from(num_logs + tx_log_idx)),
            removed: false,
        };
        res_receipt.logs.push(rpclog);
    }

    Some(res_receipt)
}

#[cfg(test)]
mod test {
    use super::*;
    use reth_db::database::Database;
    use reth_primitives::{
        address, b256, bloom, Block, Bytes, Log, Receipts, SealedBlockWithSenders, TxType, U8,
    };
    use reth_provider::{BlockWriter, BundleStateWithReceipts, DatabaseProvider};
    use reth_revm::revm::db::BundleState;
    use std::{ffi::CString, fs::File, path::Path};

    #[inline]
    fn open_receipts_testdata_db() {
        if File::open("testdata/db").is_ok() {
            return;
        }

        let db = reth_db::init_db(Path::new("testdata/db"), None).unwrap();
        let pr = DatabaseProvider::new_rw(db.tx_mut().unwrap(), MAINNET.clone());
        let block: Block = serde_json::from_str(include_str!("../testdata/dummy_block.json"))
            .expect("failed to parse dummy block");
        let block_number = block.header.number;
        let tx_sender = block.body[0]
            .recover_signer()
            .expect("failed to recover signer");

        pr.append_blocks_with_bundle_state(
            vec![SealedBlockWithSenders {
                block: block.seal_slow(),
                senders: vec![tx_sender],
            }],
            BundleStateWithReceipts::new(
                BundleState::default(),
                Receipts::from_block_receipt(vec![Receipt {
                    tx_type: TxType::EIP1559,
                    success: true,
                    cumulative_gas_used: 0x3aefc,
                    logs: vec![Log {
                        address: address!("4ce63f351597214ef0b9a319124eea9e0f9668bb"),
                        topics: vec![
                            b256!(
                                "0cdbd8bd7813095001c5fe7917bd69d834dc01db7c1dfcf52ca135bd20384413"
                            ),
                            b256!(
                                "00000000000000000000000000000000000000000000000000000000000000c2"
                            ),
                        ],
                        data: Bytes::default(),
                    }],
                }]),
                block_number,
            ),
            None,
        )
        .expect("failed to append block and receipt to database");

        pr.commit()
            .expect("failed to commit block and receipt to database");
    }

    #[test]
    fn fetch_receipts() {
        open_receipts_testdata_db();

        unsafe {
            let mut block_hash =
                b256!("bcc3fb97b87bb4b14bacde74255cbfcf52675c0ad5e06fa264c0e5d6c0afd96e");
            let receipts_res = super::read_receipts_inner(
                block_hash.as_mut_ptr(),
                32,
                CString::new("testdata/db").unwrap().into_raw() as *const c_char,
            )
            .unwrap();

            let receipts_data =
                std::slice::from_raw_parts(receipts_res.data as *const u8, receipts_res.data_len);
            let receipt = {
                let mut receipts: Vec<TransactionReceipt> =
                    serde_json::from_slice(receipts_data).unwrap();
                receipts.remove(0)
            };

            assert_eq!(receipt.transaction_type, U8::from(2));
            assert_eq!(receipt.status_code, Some(U64::from(1)));
            assert_eq!(receipt.cumulative_gas_used, U256::from(241_404));
            assert_eq!(receipt.logs_bloom, bloom!("00000000000000000000000000000000000000000100008000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000000000004000000000000000010020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040000000000000000000000800000000000000000000000000000000000000000000000000000000"));
            assert_eq!(
                receipt.logs[0].address,
                address!("4ce63f351597214ef0b9a319124eea9e0f9668bb")
            );
            assert_eq!(
                receipt.logs[0].topics[0],
                b256!("0cdbd8bd7813095001c5fe7917bd69d834dc01db7c1dfcf52ca135bd20384413")
            );
            assert_eq!(
                receipt.logs[0].topics[1],
                b256!("00000000000000000000000000000000000000000000000000000000000000c2")
            );
            assert_eq!(receipt.logs[0].data, Bytes::default());
            assert_eq!(
                receipt.from,
                address!("a24efab96523efa6abb2de9b2c16205cfa3c1dc8")
            );
            assert_eq!(
                receipt.to,
                Some(address!("4ce63f351597214ef0b9a319124eea9e0f9668bb"))
            );
            assert_eq!(
                receipt.transaction_hash,
                Some(b256!(
                    "12c0074a4a7916fe6f39de8417fe93f1fa77bcadfd5fc31a317fb6c344f66602"
                ))
            );

            assert_eq!(receipt.block_number, Some(U256::from(9_942_861)));
            assert_eq!(receipt.block_hash, Some(block_hash));
            assert_eq!(receipt.transaction_index, U64::from(0));

            crate::rdb_free_string(receipts_res.data as *mut c_char);
        }
    }
}
