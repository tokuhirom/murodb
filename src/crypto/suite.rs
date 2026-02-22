use crate::crypto::aead::{MasterKey, PageCrypto};
use crate::error::{MuroError, Result};
use crate::storage::page::PageId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionSuite {
    Plaintext,
    Aes256GcmSiv,
}

impl EncryptionSuite {
    pub const PLAINTEXT_ID: u32 = 0;
    pub const AES256_GCM_SIV_ID: u32 = 1;

    pub const fn id(self) -> u32 {
        match self {
            EncryptionSuite::Plaintext => Self::PLAINTEXT_ID,
            EncryptionSuite::Aes256GcmSiv => Self::AES256_GCM_SIV_ID,
        }
    }

    pub fn from_id(id: u32) -> Result<Self> {
        match id {
            Self::PLAINTEXT_ID => Ok(EncryptionSuite::Plaintext),
            Self::AES256_GCM_SIV_ID => Ok(EncryptionSuite::Aes256GcmSiv),
            _ => Err(MuroError::Encryption(format!(
                "unsupported encryption suite id {}",
                id
            ))),
        }
    }

    pub const fn requires_master_key(self) -> bool {
        matches!(self, EncryptionSuite::Aes256GcmSiv)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            EncryptionSuite::Plaintext => "plaintext",
            EncryptionSuite::Aes256GcmSiv => "aes256-gcm-siv",
        }
    }
}

enum CipherImpl {
    Plaintext,
    Aead(Box<PageCrypto>),
}

pub struct PageCipher {
    suite: EncryptionSuite,
    inner: CipherImpl,
}

impl PageCipher {
    pub fn new(suite: EncryptionSuite, master_key: Option<&MasterKey>) -> Result<Self> {
        let inner = match suite {
            EncryptionSuite::Plaintext => CipherImpl::Plaintext,
            EncryptionSuite::Aes256GcmSiv => {
                let key = master_key.ok_or_else(|| {
                    MuroError::Encryption(
                        "master key is required for aes256-gcm-siv encryption suite".to_string(),
                    )
                })?;
                CipherImpl::Aead(Box::new(PageCrypto::new(key)))
            }
        };

        Ok(Self { suite, inner })
    }

    pub const fn suite(&self) -> EncryptionSuite {
        self.suite
    }

    pub const fn overhead(&self) -> usize {
        match self.inner {
            CipherImpl::Plaintext => 0,
            CipherImpl::Aead(_) => PageCrypto::overhead(),
        }
    }

    pub fn encrypt(&self, page_id: PageId, epoch: u64, plaintext: &[u8]) -> Result<Vec<u8>> {
        match &self.inner {
            CipherImpl::Plaintext => Ok(plaintext.to_vec()),
            CipherImpl::Aead(c) => c.encrypt(page_id, epoch, plaintext),
        }
    }

    pub fn decrypt(&self, page_id: PageId, epoch: u64, encrypted: &[u8]) -> Result<Vec<u8>> {
        match &self.inner {
            CipherImpl::Plaintext => Ok(encrypted.to_vec()),
            CipherImpl::Aead(c) => c.decrypt(page_id, epoch, encrypted),
        }
    }

    pub fn encrypt_into(
        &self,
        page_id: PageId,
        epoch: u64,
        plaintext: &[u8],
        out: &mut [u8],
    ) -> Result<usize> {
        match &self.inner {
            CipherImpl::Plaintext => {
                if out.len() < plaintext.len() {
                    return Err(MuroError::Encryption(
                        "output buffer too small for plaintext mode".to_string(),
                    ));
                }
                out[..plaintext.len()].copy_from_slice(plaintext);
                Ok(plaintext.len())
            }
            CipherImpl::Aead(c) => c.encrypt_into(page_id, epoch, plaintext, out),
        }
    }

    pub fn decrypt_into(
        &self,
        page_id: PageId,
        epoch: u64,
        encrypted: &[u8],
        out: &mut [u8],
    ) -> Result<usize> {
        match &self.inner {
            CipherImpl::Plaintext => {
                if out.len() < encrypted.len() {
                    return Err(MuroError::Decryption);
                }
                out[..encrypted.len()].copy_from_slice(encrypted);
                Ok(encrypted.len())
            }
            CipherImpl::Aead(c) => c.decrypt_into(page_id, epoch, encrypted, out),
        }
    }
}
