use super::*;
use crate::crypto::aead::MasterKey;
use crate::storage::pager::Pager;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

fn term_key() -> [u8; 32] {
    [0x55u8; 32]
}

fn segment_payload_exists(
    idx: &mut FtsIndex,
    pager: &mut Pager,
    tid: &[u8; 32],
    generation: u32,
    seg_idx: u32,
) -> bool {
    idx.btree
        .search(pager, &seg_data_key_v2(tid, generation, seg_idx))
        .unwrap()
        .is_some()
        || idx
            .btree
            .search(pager, &seg_overflow_key_v2(tid, generation, seg_idx))
            .unwrap()
            .is_some()
}

#[test]
fn test_fts_index_add_and_search() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();

    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

    let ops = vec![
        FtsPendingOp::Add {
            doc_id: 1,
            text: "東京タワー".to_string(),
        },
        FtsPendingOp::Add {
            doc_id: 2,
            text: "東京スカイツリー".to_string(),
        },
    ];
    idx.apply_pending(&mut pager, &ops).unwrap();

    // Both documents should match "東京"
    let pl = idx.get_postings(&mut pager, "東京").unwrap();
    assert_eq!(pl.df(), 2);

    // Only doc 1 should match "タワ"
    let pl = idx.get_postings(&mut pager, "タワ").unwrap();
    assert_eq!(pl.df(), 1);
    assert_eq!(pl.get(1).unwrap().positions, vec![2]);

    let stats = idx.get_stats(&mut pager).unwrap();
    assert_eq!(stats.total_docs, 2);
}

#[test]
fn test_fts_index_remove() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();

    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

    idx.apply_pending(
        &mut pager,
        &[FtsPendingOp::Add {
            doc_id: 1,
            text: "東京タワー".to_string(),
        }],
    )
    .unwrap();

    idx.apply_pending(
        &mut pager,
        &[FtsPendingOp::Remove {
            doc_id: 1,
            text: "東京タワー".to_string(),
        }],
    )
    .unwrap();

    let pl = idx.get_postings(&mut pager, "東京").unwrap();
    assert_eq!(pl.df(), 0);

    let stats = idx.get_stats(&mut pager).unwrap();
    assert_eq!(stats.total_docs, 0);
}

#[test]
fn test_build_from_docs() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();

    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

    let docs = vec![
        (1, "東京タワーは有名です".to_string()),
        (2, "京都の寺院が美しい".to_string()),
        (3, "東京の夜景が綺麗".to_string()),
    ];
    idx.build_from_docs(&mut pager, &docs).unwrap();

    let stats = idx.get_stats(&mut pager).unwrap();
    assert_eq!(stats.total_docs, 3);

    // "東京" should match docs 1 and 3
    let pl = idx.get_postings(&mut pager, "東京").unwrap();
    assert_eq!(pl.df(), 2);
}

#[test]
fn test_fts_large_posting_list_is_segmented_without_pageoverflow() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();

    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();
    let mut ops = Vec::new();
    for doc_id in 1..=80u64 {
        ops.push(FtsPendingOp::Add {
            doc_id,
            text: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        });
    }
    idx.apply_pending(&mut pager, &ops).unwrap();

    let pl = idx.get_postings(&mut pager, "aa").unwrap();
    assert_eq!(pl.df(), 80);
}

#[test]
fn test_store_postings_spills_large_single_doc_positions_to_overflow_pages() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();
    let tid = idx.term_id("東京");

    let mut pl = PostingList::new();
    let positions: Vec<u32> = (0..5000u32).collect();
    pl.add(1, positions.clone());
    idx.store_postings_by_tid(&mut pager, &tid, &pl).unwrap();

    let loaded = idx.load_postings_by_tid(&mut pager, &tid).unwrap();
    assert_eq!(loaded.df(), 1);
    assert_eq!(loaded.get(1).unwrap().positions, positions);

    let meta = idx
        .btree
        .search(&mut pager, &seg_meta_key(&tid))
        .unwrap()
        .unwrap();
    match decode_segment_meta(&meta).unwrap() {
        SegmentMeta::V2 {
            generation,
            seg_count,
        } => {
            assert_eq!(seg_count, 1);
            assert!(idx
                .btree
                .search(&mut pager, &seg_data_key_v2(&tid, generation, 0))
                .unwrap()
                .is_none());
            let overflow_ref = decode_overflow_ref(
                &idx.btree
                    .search(&mut pager, &seg_overflow_key_v2(&tid, generation, 0))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert!(overflow_ref.page_count >= 2);
        }
        SegmentMeta::V1 { .. } => panic!("expected v2 segment metadata"),
    }
}

#[test]
fn test_get_postings_reads_legacy_single_value_format() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

    let tid = idx.term_id("東京");
    let mut pl = PostingList::new();
    pl.add(1, vec![0, 2]);
    pl.add(3, vec![1]);
    idx.btree.insert(&mut pager, &tid, &pl.serialize()).unwrap();

    let loaded = idx.get_postings(&mut pager, "東京").unwrap();
    assert_eq!(loaded.df(), 2);
    assert_eq!(loaded.get(1).unwrap().positions, vec![0, 2]);
    assert_eq!(loaded.get(3).unwrap().positions, vec![1]);
}

#[test]
fn test_get_postings_reads_legacy_segmented_u16_format() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

    let tid = idx.term_id("東京");
    let mut seg0 = PostingList::new();
    seg0.add(1, vec![0]);
    let mut seg1 = PostingList::new();
    seg1.add(2, vec![1, 2]);

    idx.btree
        .insert(&mut pager, &seg_meta_key(&tid), &(2u16).to_le_bytes())
        .unwrap();
    idx.btree
        .insert(
            &mut pager,
            &seg_data_key_legacy_u16(&tid, 0),
            &seg0.serialize(),
        )
        .unwrap();
    idx.btree
        .insert(
            &mut pager,
            &seg_data_key_legacy_u16(&tid, 1),
            &seg1.serialize(),
        )
        .unwrap();

    let loaded = idx.get_postings(&mut pager, "東京").unwrap();
    assert_eq!(loaded.df(), 2);
    assert_eq!(loaded.get(1).unwrap().positions, vec![0]);
    assert_eq!(loaded.get(2).unwrap().positions, vec![1, 2]);
}

#[test]
fn test_store_postings_migrates_to_segment_v2_without_losing_data() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

    let tid = idx.term_id("東京");
    let mut old_seg = PostingList::new();
    old_seg.add(1, vec![0]);
    idx.btree
        .insert(&mut pager, &seg_meta_key(&tid), &(1u32).to_le_bytes())
        .unwrap();
    idx.btree
        .insert(&mut pager, &seg_data_key(&tid, 0), &old_seg.serialize())
        .unwrap();

    let mut new_pl = PostingList::new();
    new_pl.add(7, vec![3, 5]);
    idx.store_postings_by_tid(&mut pager, &tid, &new_pl)
        .unwrap();

    let loaded = idx.load_postings_by_tid(&mut pager, &tid).unwrap();
    assert_eq!(loaded.df(), 1);
    assert_eq!(loaded.get(7).unwrap().positions, vec![3, 5]);

    let meta = idx
        .btree
        .search(&mut pager, &seg_meta_key(&tid))
        .unwrap()
        .unwrap();
    match decode_segment_meta(&meta).unwrap() {
        SegmentMeta::V2 {
            generation,
            seg_count,
        } => {
            assert_eq!(generation, 1);
            assert_eq!(seg_count, 1);
        }
        SegmentMeta::V1 { .. } => panic!("expected v2 segment metadata"),
    }
}

#[test]
fn test_vacuum_stale_segments_removes_previous_generation_payload() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();
    let tid = idx.term_id("東京");

    let mut pl1 = PostingList::new();
    pl1.add(1, vec![0]);
    idx.store_postings_by_tid(&mut pager, &tid, &pl1).unwrap();

    let mut pl2 = PostingList::new();
    pl2.add(2, vec![1]);
    idx.store_postings_by_tid(&mut pager, &tid, &pl2).unwrap();

    assert!(segment_payload_exists(&mut idx, &mut pager, &tid, 1, 0));

    let processed = idx.vacuum_stale_segments(&mut pager, 16).unwrap();
    assert!(processed >= 1);
    assert!(!segment_payload_exists(&mut idx, &mut pager, &tid, 1, 0));
    assert!(segment_payload_exists(&mut idx, &mut pager, &tid, 2, 0));
}

#[test]
fn test_vacuum_stale_segments_removes_legacy_single_value() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();
    let tid = idx.term_id("東京");

    let mut legacy = PostingList::new();
    legacy.add(1, vec![0]);
    idx.btree
        .insert(&mut pager, &tid, &legacy.serialize())
        .unwrap();

    let mut next = PostingList::new();
    next.add(5, vec![3]);
    idx.store_postings_by_tid(&mut pager, &tid, &next).unwrap();

    assert!(idx.btree.search(&mut pager, &tid).unwrap().is_some());
    let processed = idx.vacuum_stale_segments(&mut pager, 16).unwrap();
    assert!(processed >= 1);
    assert!(idx.btree.search(&mut pager, &tid).unwrap().is_none());
}

#[test]
fn test_generation_does_not_reuse_after_delete_then_readd() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();
    let tid = idx.term_id("東京");

    let mut pl1 = PostingList::new();
    pl1.add(1, vec![0]);
    idx.store_postings_by_tid(&mut pager, &tid, &pl1).unwrap(); // gen=1

    let mut pl2 = PostingList::new();
    pl2.add(2, vec![1]);
    idx.store_postings_by_tid(&mut pager, &tid, &pl2).unwrap(); // gen=2, queue old gen=1 GC

    let empty = PostingList::new();
    idx.store_postings_by_tid(&mut pager, &tid, &empty).unwrap(); // delete term + meta

    let mut pl3 = PostingList::new();
    pl3.add(3, vec![2]);
    idx.store_postings_by_tid(&mut pager, &tid, &pl3).unwrap(); // must become gen=3 (not reused 1)

    let meta = idx
        .btree
        .search(&mut pager, &seg_meta_key(&tid))
        .unwrap()
        .unwrap();
    match decode_segment_meta(&meta).unwrap() {
        SegmentMeta::V2 {
            generation,
            seg_count,
        } => {
            assert_eq!(generation, 3);
            assert_eq!(seg_count, 1);
        }
        SegmentMeta::V1 { .. } => panic!("expected v2 segment metadata"),
    }

    let processed = idx.vacuum_stale_segments(&mut pager, 64).unwrap();
    assert!(processed >= 1);

    let loaded = idx.load_postings_by_tid(&mut pager, &tid).unwrap();
    assert_eq!(loaded.df(), 1);
    assert_eq!(loaded.get(3).unwrap().positions, vec![2]);
}
