---- MODULE CrashResilience ----
EXTENDS Naturals, FiniteSets, Sequences, TLC

(***************************************************************************)
(* Abstraction                                                             *)
(* - Model commit pipeline at tx granularity (Begin/Write/Meta/Commit).   *)
(* - WAL is modeled as durable per-transaction summaries + commit order.   *)
(* - Crash discards only volatile in-flight transaction state.             *)
(* - Recover replays committed WAL transactions to reconstruct state.       *)
(* - Freelist content (set of free page IDs) is tracked per transaction.   *)
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
  [ committed          : BOOLEAN,
    pages              : [Pages -> Values],
    touched            : SUBSET Pages,
    metaSet            : BOOLEAN,
    metaRoot           : Roots,
    metaCount          : 0..MaxPageCount,
    metaFreelist       : FreelistIds,
    metaFreelistContent: SUBSET Pages ]

VARIABLES
  mode,
  activeTx,
  bufPages,
  bufTouched,
  bufMetaSet,
  bufMetaRoot,
  bufMetaCount,
  bufMetaFreelist,
  bufMetaFreelistContent,
  walTx,
  committedOrder,
  db,
  catalogRoot,
  pageCount,
  freelistId,
  freelistContent,
  initDb,
  initCatalogRoot,
  initPageCount,
  initFreelistId,
  initFreelistContent

vars == <<
  mode,
  activeTx,
  bufPages,
  bufTouched,
  bufMetaSet,
  bufMetaRoot,
  bufMetaCount,
  bufMetaFreelist,
  bufMetaFreelistContent,
  walTx,
  committedOrder,
  db,
  catalogRoot,
  pageCount,
  freelistId,
  freelistContent,
  initDb,
  initCatalogRoot,
  initPageCount,
  initFreelistId,
  initFreelistContent
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
  /\ bufMetaFreelistContent \subseteq Pages
  /\ walTx \in [TxIds -> TxRec]
  /\ committedOrder \in Seq(TxIds)
  /\ db \in [Pages -> Values]
  /\ catalogRoot \in Roots
  /\ pageCount \in 0..MaxPageCount
  /\ freelistId \in FreelistIds
  /\ freelistContent \subseteq Pages
  /\ initDb \in [Pages -> Values]
  /\ initCatalogRoot \in Roots
  /\ initPageCount \in 0..MaxPageCount
  /\ initFreelistId \in FreelistIds
  /\ initFreelistContent \subseteq Pages

Init ==
  /\ mode = "Running"
  /\ activeTx = NoTx
  /\ bufPages \in [Pages -> Values]
  /\ bufTouched = {}
  /\ bufMetaSet = FALSE
  /\ bufMetaRoot \in Roots
  /\ bufMetaCount \in 0..MaxPageCount
  /\ bufMetaFreelist \in FreelistIds
  /\ bufMetaFreelistContent = {}
  /\ walTx = [t \in TxIds |->
      [ committed          |-> FALSE,
        pages              |-> [p \in Pages |-> CHOOSE v \in Values : TRUE],
        touched            |-> {},
        metaSet            |-> FALSE,
        metaRoot           |-> CHOOSE r \in Roots : TRUE,
        metaCount          |-> 0,
        metaFreelist       |-> CHOOSE f \in FreelistIds : TRUE,
        metaFreelistContent|-> {} ]]
  /\ committedOrder = <<>>
  /\ db \in [Pages -> Values]
  /\ catalogRoot \in Roots
  /\ pageCount \in 0..MaxPageCount
  /\ freelistId \in FreelistIds
  /\ freelistContent \subseteq Pages
  /\ initDb = db
  /\ initCatalogRoot = catalogRoot
  /\ initPageCount = pageCount
  /\ initFreelistId = freelistId
  /\ initFreelistContent = freelistContent

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
      /\ bufMetaFreelistContent' = freelistContent
  /\ UNCHANGED <<mode, walTx, committedOrder, db, catalogRoot, pageCount, freelistId, freelistContent,
                 initDb, initCatalogRoot, initPageCount, initFreelistId, initFreelistContent>>

WritePage ==
  /\ mode = "Running"
  /\ activeTx \in TxIds
  /\ \E p \in Pages, v \in Values:
      /\ bufPages' = [bufPages EXCEPT ![p] = v]
      /\ bufTouched' = bufTouched \cup {p}
  /\ UNCHANGED <<mode, activeTx, bufMetaSet, bufMetaRoot, bufMetaCount, bufMetaFreelist, bufMetaFreelistContent,
                 walTx, committedOrder, db, catalogRoot, pageCount, freelistId, freelistContent,
                 initDb, initCatalogRoot, initPageCount, initFreelistId, initFreelistContent>>

SetMeta ==
  /\ mode = "Running"
  /\ activeTx \in TxIds
  /\ \E r \in Roots, c \in 0..MaxPageCount, f \in FreelistIds, fc \in SUBSET Pages:
      /\ bufMetaSet' = TRUE
      /\ bufMetaRoot' = r
      /\ bufMetaCount' = c
      /\ bufMetaFreelist' = f
      /\ bufMetaFreelistContent' = fc
  /\ UNCHANGED <<mode, activeTx, bufPages, bufTouched,
                 walTx, committedOrder, db, catalogRoot, pageCount, freelistId, freelistContent,
                 initDb, initCatalogRoot, initPageCount, initFreelistId, initFreelistContent>>

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
        !.metaFreelist = bufMetaFreelist,
        !.metaFreelistContent = bufMetaFreelistContent ]]
  /\ committedOrder' = Append(committedOrder, activeTx)
  /\ activeTx' = NoTx
  /\ bufTouched' = {}
  /\ bufMetaSet' = FALSE
  /\ UNCHANGED <<mode, bufPages, bufMetaRoot, bufMetaCount, bufMetaFreelist, bufMetaFreelistContent,
                 db, catalogRoot, pageCount, freelistId, freelistContent,
                 initDb, initCatalogRoot, initPageCount, initFreelistId, initFreelistContent>>

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
              /\ freelistContent' = IF fm /\ walTx[t].metaSet THEN walTx[t].metaFreelistContent ELSE freelistContent
  /\ UNCHANGED <<mode, activeTx, bufPages, bufTouched, bufMetaSet, bufMetaRoot, bufMetaCount, bufMetaFreelist, bufMetaFreelistContent,
                 walTx, committedOrder, initDb, initCatalogRoot, initPageCount, initFreelistId, initFreelistContent>>

Crash ==
  /\ mode = "Running"
  /\ mode' = "Crashed"
  /\ activeTx' = NoTx
  /\ bufTouched' = {}
  /\ bufMetaSet' = FALSE
  /\ UNCHANGED <<bufPages, bufMetaRoot, bufMetaCount, bufMetaFreelist, bufMetaFreelistContent,
                 walTx, committedOrder, db, catalogRoot, pageCount, freelistId, freelistContent,
                 initDb, initCatalogRoot, initPageCount, initFreelistId, initFreelistContent>>

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

ExpectedFreelistContent ==
  IF LastMetaIndex = 0
  THEN initFreelistContent
  ELSE walTx[committedOrder[LastMetaIndex]].metaFreelistContent

Recover ==
  /\ mode = "Crashed"
  /\ mode' = "Recovered"
  /\ db' = ExpectedDb
  /\ catalogRoot' = ExpectedCatalogRoot
  /\ pageCount' \in ExpectedMinPageCount..MaxPageCount
  /\ freelistId' = ExpectedFreelistId
  /\ freelistContent' = ExpectedFreelistContent
  /\ UNCHANGED <<activeTx, bufPages, bufTouched, bufMetaSet, bufMetaRoot, bufMetaCount, bufMetaFreelist, bufMetaFreelistContent,
                 walTx, committedOrder, initDb, initCatalogRoot, initPageCount, initFreelistId, initFreelistContent>>

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

(* New invariant: freelist content matches the last committed freelist content *)
FreelistContentConsistent ==
  mode = "Recovered" => freelistContent = ExpectedFreelistContent

(* New invariant: all free pages are within the page count range *)
PageCountCoversFreelist ==
  mode = "Recovered" =>
    \A p \in freelistContent: p < pageCount

THEOREM Spec => []TypeInv
====
