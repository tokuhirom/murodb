/// WAL record types.
///
/// Record format on disk (encrypted):
///   [record_len: u32] [record_type: u8] [payload...] [crc32: u32]
///
/// Record types:
///   Begin(txid)
///   PagePut(txid, page_id, page_data)
///   Commit(txid, lsn)
///   Abort(txid)
use crate::storage::page::PageId;

pub type TxId = u64;
pub type Lsn = u64;

#[derive(Debug, Clone)]
pub enum WalRecord {
    Begin {
        txid: TxId,
    },
    PagePut {
        txid: TxId,
        page_id: PageId,
        data: Vec<u8>,
    },
    Commit {
        txid: TxId,
        lsn: Lsn,
    },
    Abort {
        txid: TxId,
    },
}

const TAG_BEGIN: u8 = 1;
const TAG_PAGE_PUT: u8 = 2;
const TAG_COMMIT: u8 = 3;
const TAG_ABORT: u8 = 4;

impl WalRecord {
    pub fn txid(&self) -> TxId {
        match self {
            WalRecord::Begin { txid } => *txid,
            WalRecord::PagePut { txid, .. } => *txid,
            WalRecord::Commit { txid, .. } => *txid,
            WalRecord::Abort { txid } => *txid,
        }
    }

    /// Serialize to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            WalRecord::Begin { txid } => {
                let mut buf = Vec::with_capacity(1 + 8);
                buf.push(TAG_BEGIN);
                buf.extend_from_slice(&txid.to_le_bytes());
                buf
            }
            WalRecord::PagePut {
                txid,
                page_id,
                data,
            } => {
                let mut buf = Vec::with_capacity(1 + 8 + 8 + 4 + data.len());
                buf.push(TAG_PAGE_PUT);
                buf.extend_from_slice(&txid.to_le_bytes());
                buf.extend_from_slice(&page_id.to_le_bytes());
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
                buf
            }
            WalRecord::Commit { txid, lsn } => {
                let mut buf = Vec::with_capacity(1 + 8 + 8);
                buf.push(TAG_COMMIT);
                buf.extend_from_slice(&txid.to_le_bytes());
                buf.extend_from_slice(&lsn.to_le_bytes());
                buf
            }
            WalRecord::Abort { txid } => {
                let mut buf = Vec::with_capacity(1 + 8);
                buf.push(TAG_ABORT);
                buf.extend_from_slice(&txid.to_le_bytes());
                buf
            }
        }
    }

    /// Deserialize from bytes.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        match data[0] {
            TAG_BEGIN => {
                if data.len() < 9 {
                    return None;
                }
                let txid = u64::from_le_bytes(data[1..9].try_into().unwrap());
                Some(WalRecord::Begin { txid })
            }
            TAG_PAGE_PUT => {
                if data.len() < 21 {
                    return None;
                }
                let txid = u64::from_le_bytes(data[1..9].try_into().unwrap());
                let page_id = u64::from_le_bytes(data[9..17].try_into().unwrap());
                let data_len = u32::from_le_bytes(data[17..21].try_into().unwrap()) as usize;
                if data.len() < 21 + data_len {
                    return None;
                }
                let page_data = data[21..21 + data_len].to_vec();
                Some(WalRecord::PagePut {
                    txid,
                    page_id,
                    data: page_data,
                })
            }
            TAG_COMMIT => {
                if data.len() < 17 {
                    return None;
                }
                let txid = u64::from_le_bytes(data[1..9].try_into().unwrap());
                let lsn = u64::from_le_bytes(data[9..17].try_into().unwrap());
                Some(WalRecord::Commit { txid, lsn })
            }
            TAG_ABORT => {
                if data.len() < 9 {
                    return None;
                }
                let txid = u64::from_le_bytes(data[1..9].try_into().unwrap());
                Some(WalRecord::Abort { txid })
            }
            _ => None,
        }
    }
}

/// Simple CRC32 for record integrity (not cryptographic, just corruption detection).
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_roundtrip() {
        let records = vec![
            WalRecord::Begin { txid: 1 },
            WalRecord::PagePut {
                txid: 1,
                page_id: 42,
                data: vec![0xAB; 100],
            },
            WalRecord::Commit { txid: 1, lsn: 5 },
            WalRecord::Abort { txid: 2 },
        ];

        for record in &records {
            let serialized = record.serialize();
            let deserialized = WalRecord::deserialize(&serialized).unwrap();
            assert_eq!(record.txid(), deserialized.txid());
        }
    }

    #[test]
    fn test_crc32() {
        let data = b"hello world";
        let c1 = crc32(data);
        let c2 = crc32(data);
        assert_eq!(c1, c2);
        assert_ne!(crc32(b"hello world"), crc32(b"hello worle"));
    }
}
