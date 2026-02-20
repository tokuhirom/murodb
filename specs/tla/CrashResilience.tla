---- MODULE CrashResilience ----
EXTENDS Naturals, FiniteSets, Sequences, TLC

(***************************************************************************)
(* Abstraction                                                             *)
(* - Model commit pipeline at tx granularity (Begin/Write/Meta/Commit).   *)
(* - WAL is modeled as durable per-transaction summaries + commit order.   *)
(* - Crash discards only volatile in-flight transaction state.             *)
(* - Recover replays committed WAL transactions to reconstruct state.       *)
(***************************************************************************)

CONSTANTS
  TxIds,
  Pages,
  Values,
  Roots,
  FreelistIds,
  NoTx

ASSUME TxIds /= {}
ASSUME Pages /= {}
ASSUME Values /= {}
ASSUME Roots /= {}
ASSUME FreelistIds /= {}
ASSUME NoTx \notin TxIds

MaxPageCount == Cardinality(Pages)

MaxNat(S) ==
  CHOOSE m \in S : \A x \in S : x <= m

TxRec ==
  [ committed    : BOOLEAN,
    pages        : [Pages -> Values],
    touched      : SUBSET Pages,
    metaSet      : BOOLEAN,
    metaRoot     : Roots,
    metaCount    : 0..MaxPageCount,
    metaFreelist : FreelistIds ]

VARIABLES
  mode,
  activeTx,
  bufPages,
  bufTouched,
  bufMetaSet,
  bufMetaRoot,
  bufMetaCount,
  bufMetaFreelist,
  walTx,
  committedOrder,
  db,
  catalogRoot,
  pageCount,
  freelistId,
  initDb,
  initCatalogRoot,
  initPageCount,
  initFreelistId

vars == <<
  mode,
  activeTx,
  bufPages,
  bufTouched,
  bufMetaSet,
  bufMetaRoot,
  bufMetaCount,
  bufMetaFreelist,
  walTx,
  committedOrder,
  db,
  catalogRoot,
  pageCount,
  freelistId,
  initDb,
  initCatalogRoot,
  initPageCount,
  initFreelistId
>>

TypeInv ==
  /\ mode \in {"Running", "Crashed", "Recovered"}
  /\ activeTx \in TxIds \cup {NoTx}
  /\ bufPages \in [Pages -> Values]
  /\ bufTouched \subseteq Pages
  /\ bufMetaSet \in BOOLEAN
  /\ bufMetaRoot \in Roots
  /\ bufMetaCount \in 0..MaxPageCount
  /\ bufMetaFreelist \in FreelistIds
  /\ walTx \in [TxIds -> TxRec]
  /\ committedOrder \in Seq(TxIds)
  /\ db \in [Pages -> Values]
  /\ catalogRoot \in Roots
  /\ pageCount \in 0..MaxPageCount
  /\ freelistId \in FreelistIds
  /\ initDb \in [Pages -> Values]
  /\ initCatalogRoot \in Roots
  /\ initPageCount \in 0..MaxPageCount
  /\ initFreelistId \in FreelistIds

Init ==
  /\ mode = "Running"
  /\ activeTx = NoTx
  /\ bufPages \in [Pages -> Values]
  /\ bufTouched = {}
  /\ bufMetaSet = FALSE
  /\ bufMetaRoot \in Roots
  /\ bufMetaCount \in 0..MaxPageCount
  /\ bufMetaFreelist \in FreelistIds
  /\ walTx = [t \in TxIds |->
      [ committed    |-> FALSE,
        pages        |-> [p \in Pages |-> CHOOSE v \in Values : TRUE],
        touched      |-> {},
        metaSet      |-> FALSE,
        metaRoot     |-> CHOOSE r \in Roots : TRUE,
        metaCount    |-> 0,
        metaFreelist |-> CHOOSE f \in FreelistIds : TRUE ]]
  /\ committedOrder = <<>>
  /\ db \in [Pages -> Values]
  /\ catalogRoot \in Roots
  /\ pageCount \in 0..MaxPageCount
  /\ freelistId \in FreelistIds
  /\ initDb = db
  /\ initCatalogRoot = catalogRoot
  /\ initPageCount = pageCount
  /\ initFreelistId = freelistId

BeginTx ==
  /\ mode = "Running"
  /\ activeTx = NoTx
  /\ \E t \in TxIds:
      /\ ~walTx[t].committed
      /\ activeTx' = t
      /\ bufPages' = [p \in Pages |-> db[p]]
      /\ bufTouched' = {}
      /\ bufMetaSet' = FALSE
      /\ bufMetaRoot' = catalogRoot
      /\ bufMetaCount' = pageCount
      /\ bufMetaFreelist' = freelistId
  /\ UNCHANGED <<mode, walTx, committedOrder, db, catalogRoot, pageCount, freelistId,
                 initDb, initCatalogRoot, initPageCount, initFreelistId>>

WritePage ==
  /\ mode = "Running"
  /\ activeTx \in TxIds
  /\ \E p \in Pages, v \in Values:
      /\ bufPages' = [bufPages EXCEPT ![p] = v]
      /\ bufTouched' = bufTouched \cup {p}
  /\ UNCHANGED <<mode, activeTx, bufMetaSet, bufMetaRoot, bufMetaCount, bufMetaFreelist,
                 walTx, committedOrder, db, catalogRoot, pageCount, freelistId,
                 initDb, initCatalogRoot, initPageCount, initFreelistId>>

SetMeta ==
  /\ mode = "Running"
  /\ activeTx \in TxIds
  /\ \E r \in Roots, c \in 0..MaxPageCount, f \in FreelistIds:
      /\ bufMetaSet' = TRUE
      /\ bufMetaRoot' = r
      /\ bufMetaCount' = c
      /\ bufMetaFreelist' = f
  /\ UNCHANGED <<mode, activeTx, bufPages, bufTouched,
                 walTx, committedOrder, db, catalogRoot, pageCount, freelistId,
                 initDb, initCatalogRoot, initPageCount, initFreelistId>>

DurableCommit ==
  /\ mode = "Running"
  /\ activeTx \in TxIds
  /\ bufMetaSet = TRUE
  /\ walTx' = [walTx EXCEPT
      ![activeTx] = [@ EXCEPT
        !.committed = TRUE,
        !.pages = bufPages,
        !.touched = bufTouched,
        !.metaSet = bufMetaSet,
        !.metaRoot = bufMetaRoot,
        !.metaCount = bufMetaCount,
        !.metaFreelist = bufMetaFreelist ]]
  /\ committedOrder' = Append(committedOrder, activeTx)
  /\ activeTx' = NoTx
  /\ bufTouched' = {}
  /\ bufMetaSet' = FALSE
  /\ UNCHANGED <<mode, bufPages, bufMetaRoot, bufMetaCount, bufMetaFreelist,
                 db, catalogRoot, pageCount, freelistId,
                 initDb, initCatalogRoot, initPageCount, initFreelistId>>

(* Optional data-file flush before crash: any subset of a committed tx may be applied. *)
FlushSomeCommitted ==
  /\ mode = "Running"
  /\ \E t \in TxIds:
      /\ walTx[t].committed
      /\ \E s \in SUBSET walTx[t].touched:
          /\ db' = [p \in Pages |-> IF p \in s THEN walTx[t].pages[p] ELSE db[p]]
          /\ \E fm \in BOOLEAN:
              /\ catalogRoot' = IF fm /\ walTx[t].metaSet THEN walTx[t].metaRoot ELSE catalogRoot
              /\ pageCount' = IF fm /\ walTx[t].metaSet
                              THEN IF walTx[t].metaCount > pageCount THEN walTx[t].metaCount ELSE pageCount
                              ELSE pageCount
              /\ freelistId' = IF fm /\ walTx[t].metaSet THEN walTx[t].metaFreelist ELSE freelistId
  /\ UNCHANGED <<mode, activeTx, bufPages, bufTouched, bufMetaSet, bufMetaRoot, bufMetaCount, bufMetaFreelist,
                 walTx, committedOrder, initDb, initCatalogRoot, initPageCount, initFreelistId>>

Crash ==
  /\ mode = "Running"
  /\ mode' = "Crashed"
  /\ activeTx' = NoTx
  /\ bufTouched' = {}
  /\ bufMetaSet' = FALSE
  /\ UNCHANGED <<bufPages, bufMetaRoot, bufMetaCount, bufMetaFreelist,
                 walTx, committedOrder, db, catalogRoot, pageCount, freelistId,
                 initDb, initCatalogRoot, initPageCount, initFreelistId>>

LastIndexTouching(p) ==
  LET Is == {i \in 1..Len(committedOrder) : p \in walTx[committedOrder[i]].touched}
  IN IF Is = {} THEN 0 ELSE MaxNat(Is)

HasCommittedMeta ==
  \E i \in 1..Len(committedOrder): walTx[committedOrder[i]].metaSet

LastMetaIndex ==
  LET Is == {i \in 1..Len(committedOrder) : walTx[committedOrder[i]].metaSet}
  IN IF Is = {} THEN 0 ELSE MaxNat(Is)

ExpectedDb ==
  [p \in Pages |->
    IF LastIndexTouching(p) = 0
    THEN initDb[p]
    ELSE walTx[committedOrder[LastIndexTouching(p)]].pages[p]]

ExpectedCatalogRoot ==
  IF LastMetaIndex = 0
  THEN initCatalogRoot
  ELSE walTx[committedOrder[LastMetaIndex]].metaRoot

ExpectedMinPageCount ==
  LET touchedMax ==
        IF \E p \in Pages : LastIndexTouching(p) > 0
        THEN MaxNat({q + 1 : q \in {p \in Pages : LastIndexTouching(p) > 0}})
        ELSE 0
      metaMax ==
        IF LastMetaIndex = 0
        THEN initPageCount
        ELSE walTx[committedOrder[LastMetaIndex]].metaCount
  IN MaxNat({initPageCount, metaMax, touchedMax})

ExpectedFreelistId ==
  IF LastMetaIndex = 0
  THEN initFreelistId
  ELSE walTx[committedOrder[LastMetaIndex]].metaFreelist

Recover ==
  /\ mode = "Crashed"
  /\ mode' = "Recovered"
  /\ db' = ExpectedDb
  /\ catalogRoot' = ExpectedCatalogRoot
  /\ pageCount' \in ExpectedMinPageCount..MaxPageCount
  /\ freelistId' = ExpectedFreelistId
  /\ UNCHANGED <<activeTx, bufPages, bufTouched, bufMetaSet, bufMetaRoot, bufMetaCount, bufMetaFreelist,
                 walTx, committedOrder, initDb, initCatalogRoot, initPageCount, initFreelistId>>

Next ==
  BeginTx \/ WritePage \/ SetMeta \/ DurableCommit \/ FlushSomeCommitted \/ Crash \/ Recover

Spec == Init /\ [][Next]_vars /\ WF_vars(Crash) /\ WF_vars(Recover)

RecoveredSound ==
  mode = "Recovered" =>
    /\ db = ExpectedDb
    /\ catalogRoot = ExpectedCatalogRoot
    /\ pageCount >= ExpectedMinPageCount

NoUncommittedInfluence ==
  mode = "Recovered" =>
    \A t \in TxIds:
      ~walTx[t].committed =>
        \A p \in walTx[t].touched:
          LastIndexTouching(p) = 0 \/ committedOrder[LastIndexTouching(p)] # t

CommitRequiresMeta ==
  \A t \in TxIds:
    walTx[t].committed => walTx[t].metaSet

UniqueCommittedOrder ==
  \A i, j \in 1..Len(committedOrder):
    i # j => committedOrder[i] # committedOrder[j]

FreelistPreserved ==
  mode = "Recovered" => freelistId = ExpectedFreelistId

THEOREM Spec => []TypeInv
====
