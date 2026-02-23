/// Overflow page chain for storing large values that exceed a single B-tree page.
///
/// Overflow page layout (raw Page data, NOT slotted):
///   [page_id: u64]          bytes 0..8   (standard page header)
///   [0xFF marker: u8]       byte 8       (distinguishes from B-tree node pages)
///   [next_page: u64]        bytes 9..17  (next page in chain, u64::MAX = end)
///   [chunk_len: u16]        bytes 17..19 (length of data chunk in this page)
///   [chunk data]            bytes 19..19+chunk_len
///
/// Max chunk per page: 4096 - 19 = 4077 bytes.
use crate::error::{MuroError, Result};
use crate::storage::page::{Page, PageId, PAGE_SIZE};
use crate::storage::page_store::PageStore;

const OVERFLOW_MARKER: u8 = 0xFF;
const NO_NEXT_PAGE: u64 = u64::MAX;

/// Sentinel value for "no overflow chain" in overflow cells where all data fits inline.
pub const NO_OVERFLOW_PAGE: u64 = u64::MAX;
const OVERFLOW_HEADER_SIZE: usize = 19; // page_id(8) + marker(1) + next_page(8) + chunk_len(2)
pub const OVERFLOW_CHUNK_SIZE: usize = PAGE_SIZE - OVERFLOW_HEADER_SIZE; // 4077

/// Write data into an overflow page chain. Returns the first page ID.
pub fn write_overflow_chain(pager: &mut impl PageStore, data: &[u8]) -> Result<PageId> {
    if data.is_empty() {
        return Err(MuroError::Internal(
            "cannot write empty overflow chain".into(),
        ));
    }

    // Split data into chunks and allocate pages
    let chunks: Vec<&[u8]> = data.chunks(OVERFLOW_CHUNK_SIZE).collect();
    let mut pages: Vec<Page> = Vec::with_capacity(chunks.len());

    for _ in 0..chunks.len() {
        pages.push(pager.allocate_page()?);
    }

    let first_page_id = pages[0].page_id();

    // Collect page IDs upfront to avoid borrow conflicts
    let page_ids: Vec<PageId> = pages.iter().map(|p| p.page_id()).collect();

    for (i, chunk) in chunks.iter().enumerate() {
        let page_id = page_ids[i];
        let next_page_id = if i + 1 < page_ids.len() {
            page_ids[i + 1]
        } else {
            NO_NEXT_PAGE // end of chain
        };

        // Write overflow page layout directly into page data
        let page = &mut pages[i];
        page.data[0..8].copy_from_slice(&page_id.to_le_bytes());
        page.data[8] = OVERFLOW_MARKER;
        page.data[9..17].copy_from_slice(&next_page_id.to_le_bytes());
        page.data[17..19].copy_from_slice(&(chunk.len() as u16).to_le_bytes());
        page.data[19..19 + chunk.len()].copy_from_slice(chunk);
    }

    // Write all pages to disk
    for page in &pages {
        pager.write_page(page)?;
    }

    Ok(first_page_id)
}

/// Read the full value from an overflow chain.
pub fn read_overflow_chain(
    pager: &mut impl PageStore,
    first_page_id: PageId,
    total_len: u32,
) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(total_len as usize);
    let remaining = total_len as usize;

    let mut current_page_id = first_page_id;
    let mut bytes_read = 0usize;

    while current_page_id != NO_NEXT_PAGE && bytes_read < remaining {
        let page = pager.read_page(current_page_id)?;

        if page.data[8] != OVERFLOW_MARKER {
            return Err(MuroError::Corruption(format!(
                "overflow page {} has invalid marker 0x{:02X}",
                current_page_id, page.data[8]
            )));
        }

        let next_page_id = u64::from_le_bytes(page.data[9..17].try_into().unwrap());
        let chunk_len = u16::from_le_bytes(page.data[17..19].try_into().unwrap()) as usize;

        let to_read = chunk_len.min(remaining - bytes_read);
        result.extend_from_slice(&page.data[19..19 + to_read]);
        bytes_read += to_read;

        current_page_id = next_page_id;
    }

    if result.len() != total_len as usize {
        return Err(MuroError::Corruption(format!(
            "overflow chain incomplete: expected {} bytes, got {}",
            total_len,
            result.len()
        )));
    }

    Ok(result)
}

/// Free all pages in an overflow chain.
pub fn free_overflow_chain(pager: &mut impl PageStore, first_page_id: PageId) -> Result<()> {
    let page_ids = collect_overflow_pages(pager, first_page_id)?;
    for page_id in page_ids {
        pager.free_page(page_id);
    }
    Ok(())
}

/// Collect all page IDs in an overflow chain.
pub fn collect_overflow_pages(
    pager: &mut impl PageStore,
    first_page_id: PageId,
) -> Result<Vec<PageId>> {
    let mut pages = Vec::new();
    let mut current_page_id = first_page_id;

    while current_page_id != NO_NEXT_PAGE {
        if pages.contains(&current_page_id) {
            return Err(MuroError::Corruption(format!(
                "overflow chain cycle detected at page {}",
                current_page_id
            )));
        }
        pages.push(current_page_id);

        let page = pager.read_page(current_page_id)?;
        if page.data[8] != OVERFLOW_MARKER {
            return Err(MuroError::Corruption(format!(
                "overflow page {} has invalid marker 0x{:02X}",
                current_page_id, page.data[8]
            )));
        }
        current_page_id = u64::from_le_bytes(page.data[9..17].try_into().unwrap());
    }

    Ok(pages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aead::MasterKey;
    use crate::storage::pager::Pager;
    use tempfile::NamedTempFile;

    fn setup() -> (Pager, std::path::PathBuf) {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        drop(tmp);
        std::fs::remove_file(&path).ok();
        let key = MasterKey::new([0x42u8; 32]);
        let pager = Pager::create(&path, &key).unwrap();
        (pager, path)
    }

    #[test]
    fn test_small_overflow_chain() {
        let (mut pager, path) = setup();
        let data = vec![0xABu8; 100];

        let first_page = write_overflow_chain(&mut pager, &data).unwrap();
        let result = read_overflow_chain(&mut pager, first_page, data.len() as u32).unwrap();
        assert_eq!(result, data);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_multi_page_overflow_chain() {
        let (mut pager, path) = setup();
        // 10000 bytes needs 3 pages (4077 + 4077 + 1846)
        let data = vec![0xCDu8; 10000];

        let first_page = write_overflow_chain(&mut pager, &data).unwrap();
        let pages = collect_overflow_pages(&mut pager, first_page).unwrap();
        assert_eq!(pages.len(), 3);

        let result = read_overflow_chain(&mut pager, first_page, data.len() as u32).unwrap();
        assert_eq!(result, data);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_free_overflow_chain() {
        let (mut pager, path) = setup();
        let data = vec![0xABu8; 10000];

        let first_page = write_overflow_chain(&mut pager, &data).unwrap();
        let pages = collect_overflow_pages(&mut pager, first_page).unwrap();
        assert_eq!(pages.len(), 3);

        free_overflow_chain(&mut pager, first_page).unwrap();

        std::fs::remove_file(&path).ok();
    }
}
