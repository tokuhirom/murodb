/// Posting list: term_id -> [(doc_id, positions)]
/// Compression: delta encoding + varint for positions.
/// A posting entry for a single document.
#[derive(Debug, Clone, PartialEq)]
pub struct Posting {
    pub doc_id: u64,
    pub positions: Vec<u32>,
}

/// A posting list for a single term.
#[derive(Debug, Clone, Default)]
pub struct PostingList {
    pub postings: Vec<Posting>,
}

impl PostingList {
    pub fn new() -> Self {
        PostingList {
            postings: Vec::new(),
        }
    }

    /// Add a posting for a document.
    pub fn add(&mut self, doc_id: u64, positions: Vec<u32>) {
        // Keep sorted by doc_id
        match self.postings.binary_search_by_key(&doc_id, |p| p.doc_id) {
            Ok(idx) => {
                // Merge positions
                self.postings[idx].positions.extend(positions);
                self.postings[idx].positions.sort();
                self.postings[idx].positions.dedup();
            }
            Err(idx) => {
                self.postings.insert(idx, Posting { doc_id, positions });
            }
        }
    }

    /// Remove a document from the posting list.
    pub fn remove(&mut self, doc_id: u64) {
        if let Ok(idx) = self.postings.binary_search_by_key(&doc_id, |p| p.doc_id) {
            self.postings.remove(idx);
        }
    }

    /// Get posting for a specific document.
    pub fn get(&self, doc_id: u64) -> Option<&Posting> {
        self.postings
            .binary_search_by_key(&doc_id, |p| p.doc_id)
            .ok()
            .map(|idx| &self.postings[idx])
    }

    /// Document frequency (number of documents containing this term).
    pub fn df(&self) -> usize {
        self.postings.len()
    }

    /// Serialize posting list with delta + varint compression.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Number of postings
        encode_varint(&mut buf, self.postings.len() as u64);

        let mut prev_doc_id = 0u64;
        for posting in &self.postings {
            // Delta-encoded doc_id
            let delta = posting.doc_id - prev_doc_id;
            encode_varint(&mut buf, delta);
            prev_doc_id = posting.doc_id;

            // Positions count
            encode_varint(&mut buf, posting.positions.len() as u64);

            // Delta-encoded positions
            let mut prev_pos = 0u32;
            for &pos in &posting.positions {
                let delta = pos - prev_pos;
                encode_varint(&mut buf, delta as u64);
                prev_pos = pos;
            }
        }

        buf
    }

    /// Deserialize posting list.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        let mut offset = 0;

        let count = decode_varint(data, &mut offset)? as usize;
        let mut postings = Vec::with_capacity(count);

        let mut prev_doc_id = 0u64;
        for _ in 0..count {
            let delta = decode_varint(data, &mut offset)?;
            let doc_id = prev_doc_id + delta;
            prev_doc_id = doc_id;

            let pos_count = decode_varint(data, &mut offset)? as usize;
            let mut positions = Vec::with_capacity(pos_count);
            let mut prev_pos = 0u32;
            for _ in 0..pos_count {
                let delta = decode_varint(data, &mut offset)? as u32;
                let pos = prev_pos + delta;
                positions.push(pos);
                prev_pos = pos;
            }

            postings.push(Posting { doc_id, positions });
        }

        Some(PostingList { postings })
    }

    /// Merge another posting list into this one.
    pub fn merge(&mut self, other: &PostingList) {
        for posting in &other.postings {
            self.add(posting.doc_id, posting.positions.clone());
        }
    }
}

/// Encode a u64 as a varint (LEB128).
pub fn encode_varint(buf: &mut Vec<u8>, mut val: u64) {
    loop {
        let mut byte = (val & 0x7F) as u8;
        val >>= 7;
        if val != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if val == 0 {
            break;
        }
    }
}

/// Decode a varint. Returns the value and advances offset.
pub fn decode_varint(data: &[u8], offset: &mut usize) -> Option<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        if *offset >= data.len() {
            return None;
        }
        let byte = data[*offset];
        *offset += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_posting_list_add_and_get() {
        let mut pl = PostingList::new();
        pl.add(1, vec![0, 3, 5]);
        pl.add(2, vec![1, 4]);

        assert_eq!(pl.df(), 2);
        assert_eq!(pl.get(1).unwrap().positions, vec![0, 3, 5]);
        assert_eq!(pl.get(2).unwrap().positions, vec![1, 4]);
        assert!(pl.get(3).is_none());
    }

    #[test]
    fn test_posting_list_remove() {
        let mut pl = PostingList::new();
        pl.add(1, vec![0]);
        pl.add(2, vec![1]);
        pl.remove(1);
        assert_eq!(pl.df(), 1);
        assert!(pl.get(1).is_none());
    }

    #[test]
    fn test_posting_list_serialize_roundtrip() {
        let mut pl = PostingList::new();
        pl.add(1, vec![0, 3, 7]);
        pl.add(5, vec![1, 2, 10]);
        pl.add(100, vec![0]);

        let data = pl.serialize();
        let pl2 = PostingList::deserialize(&data).unwrap();

        assert_eq!(pl2.df(), 3);
        assert_eq!(pl2.get(1).unwrap().positions, vec![0, 3, 7]);
        assert_eq!(pl2.get(5).unwrap().positions, vec![1, 2, 10]);
        assert_eq!(pl2.get(100).unwrap().positions, vec![0]);
    }

    #[test]
    fn test_varint_roundtrip() {
        for val in [0u64, 1, 127, 128, 255, 300, 16384, u64::MAX] {
            let mut buf = Vec::new();
            encode_varint(&mut buf, val);
            let mut offset = 0;
            let decoded = decode_varint(&buf, &mut offset).unwrap();
            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn test_posting_list_merge() {
        let mut pl1 = PostingList::new();
        pl1.add(1, vec![0, 1]);
        pl1.add(3, vec![2]);

        let mut pl2 = PostingList::new();
        pl2.add(1, vec![5]); // merge into existing doc
        pl2.add(2, vec![0]); // new doc

        pl1.merge(&pl2);
        assert_eq!(pl1.df(), 3);
        assert_eq!(pl1.get(1).unwrap().positions, vec![0, 1, 5]);
    }

    #[test]
    fn test_empty_posting_list() {
        let pl = PostingList::new();
        let data = pl.serialize();
        let pl2 = PostingList::deserialize(&data).unwrap();
        assert_eq!(pl2.df(), 0);
    }
}
