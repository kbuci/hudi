<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# RFC-79: Improving reliability of concurrent table service executions and rollbacks

## Proposers

- @kbuci
- @suryaprasanna
- @nsivabalan

## Approvers

## Status

JIRA: HUDI-7946


## Abstract
In order to improve latency/throughput of writes into a HUDI dataset, HUDI does not require that table service operations (such as clustering and compaction) be serially and sequentially performed before/after an ingestion write. Instead, by enabling HUDI multiwriter and async table service execution, a user can orchesterate seperate writers to potentially execute table service plans concurrently to an ingestion writers.  Even though HUDI has multi writer capability since 0.7, these multiwriter semantics focused on ensuring correct execution of concurrent ingestion writers; not interleaving operations of ingestion writes, table services mad rollbacks. To expand the capability of HUDI to assist users to manage several table services for thousands of tables, we might need add support to ensure concurrent workers can reliably execute multiple table services for a dataset.
Once we have this support, it should be feasible to build orchestration for all HUDI tables in a data-lake using a centralized framework. This RFC proposes to add such capability, where any table service worker can perform table services safely alongside other table service workers. 


## Background
### Multiwriter and rollbacks
When multi-writer support was added to Hudi, a transaction manager and heartbet mechanism was introduced. The transaction manager was added to enforce with acquiring and releasing a table-level lock. the Heartbeat mechanism was introduced to track the liveness of a transaction. If the heartbest of an incomplete commit of interest has expired, HUDI can assume that said instant has failed. If not, the commit of interest could still be making progress by another writer. Rollbacks in singler writer mode is straight forward as any pending instant in timeline is deduced to have been failed and is eligible to be rolledback. In case of multi-writers, heartbest emitted by the actual writer is used for health check purposes. Hudi detects all such incomplete instants with a non-active heartbeat, and will perform rollbacks of all (heartbeat) expired instants. A rollback of https://hudi.apache.org/docs/next/rollbacks/#rolling-back-of-partially-failed-commits-w-multi-writers of a failed ingestion instant involves scheduling a rollback plan (if one doesn't exist already) for the instant and executing it, deleting any data files written, and then finally removing the instant's files in the active timeline. Creating a plan and following this ordering ensures that even if a writer crashed mid-way during a rollback, the next attempt will correctly resume the rollback and take it to completion. This RFC will explore applying some of these semantics to table service operations.

### Execution of table services 
The table service operations compact and cluster are by default "immutable" plans, meaning that once a plan is scheduled it will stay as as a pending instant until a caller invokes the table service execute API on the table service instant and sucessfully completes it. Specifically, if an inflight execution fails after transitioning the instant to inflight, the next execution attempt will implictly create and execute a rollback plan (which will delete all new instant/data files), but will keep the table service plan. This process will repeat until the instant is completed. The below visualization captures these transitions at a high level 

![table service lifecycle (1)](https://github.com/user-attachments/assets/4a656bde-4046-4d37-9398-db96144207aa)


If an immutable table service plan is configured for async execution then each of the listed table service instant transitions can potentially be preformed by independent concurrent jobs. Typically this is expected only if an execution attempt fails after performing a step, and from there the next execution attempt performs a rollback and re-attempts creating data files and comitting the instant. But in a distributed orchestration envrionment there may be a scenario where multiple jobs attempt to execute a table service plan at the same time, leading to one job rolling back a table service inflight instant while another is still executing it. This can undo the progress of table service execution (and in the worst case leave the dataset in an invalid state), harming the reliability of executing table services in an async manner. Given this use case, HUDI should enforce that at most on writer will execute a table service plan, and therefore if a writer sees a plan already being executed it should self-abort.
This would reduce the operational overhead and complexity of deploying a large-scale HUDI writer orchestration. As a result it may be a worthwhile tradeoff to update HUDI to improve reliability of table service executions in these scenarios if the resulting added complexity to HUDI can be minimized.

### Exploring "cancellable" table service plans
As of https://github.com/kbuci/hudi/blob/rfc-79/rfc/rfc-79/rfc-79-2.md the concept of a "cancellable"/"mutable" table service is proposed to be supported by HUDI. Mutable table service plans are desired for users that don't want to have a clustering plan "block" an ingestion write (and instead have a clustering instant fail and be rolled back if an ingestion write targets the same files) or that may want to have a contingency to completely remove a table service plan if it cannot be executed to completion (due needing to many resources/time or having an invalid clustering strategy). Similar to an immutable table service plan, a mutable table service plan is scheduled on the timeline and executed by a table service execution API. The difference though is that if an execution attempt fails after transitioning a mutable table service plan to inflight, the subsequent rollback will not leave the plan on the timeline but will remove it during rollback (similar to rollback of ingestion write instants like upsert commits). In addition, when the clean table service is configured to be lazy and performs a rollback of failed writes, any pending mutable table service plan will be cacnelled and cleaned up (similar to how rollback of failed writes targets pending ingestion write instants). The RFC https://github.com/kbuci/hudi/blob/rfc-79/rfc/rfc-79/rfc-79-2.md details the states of a "mutable" table service plan, and the execution of a mutable table service plan can be pictured at a high-level below:


Mutable table service plan face the same exeuction/rollback scenario mentiond above for immutable table service, where one job can execute and write data files while another job (running clean) may roll it back. Additionally, if a job schedules a mutable table service plan with an async configuration, it may be possible that the plan is left ont the timeline for an unbounded amount of time, due to table service execution repeatdily failing or being delayed. Since a pending table service plan can cause storage/performance degredations (by blocking other table services like clean that would remove/optimize files in the data partitions/timeline), a user may want to ensure that these pending/failed plans are eventually automatically cleaned up by configured clean to be lazy. Unfortunately a lazily-configured clean job can rollback a mutable table service plan before it has a chance to be executed, deleting the plan from the timeline. This is because this plan will not have an active heartbeat, due to fact that jobs that schedule or execute table service plans do not currently start a heartbeat, and even if they did the duration of time between a plan being scheduled and being picked up by an async table service plan can be longer than the heartbeat expiration threshold. This rollback will prevent the table service from ever being executed. 
To summarize, mutable table service plans not only incur a similar reliability isue as immutable plans, but, if implemented and configured in an async manner, another type of issue of being stuck on the timeline as a pending instant or being prematurely rolled back.
If mutable table services plans end up being supported, then the same reliability improvments to concurrent execution of immutable table service plans can be added for mutable table service plans as well, in addition to this issue with async mutable plan table service execution/clean.

## Design 
### Supporting concurrent executions of immutable table service plans with heartbeating
As mentioned above, once a clustering/compact "immutable" table service plan is scheduled, a corresponding cluster/compact API is called to (optionally rollback) and execute the plan. Implementing a "guard" to allow only one job to invoke the "execute" API on a table service plan (at a given time) would avoid any scenario of multiple jobs concurrently creating/deleting data files in a conflicting manner. This guard can be implemented by using HUDI heaertbeating in conjunction with the transaction manager. Whenever a job calls the compact/cluster API for an immutable table service, it can start a heartbeat to indicate to concurrent jobs that this table service already has an active execution attempt. These other jobs can then self-abort instead of starting their own (concurent) execution attempt. If the current job executing the table service plan fails, after its heartbeat expires another job can re-start the active heartbeat and start executing this plan again. 

Specifically, when invoking the clustering/compact API to execute an existing immutable table service plan, the aforementioned heartbeat guard will be implemented by running the following steps before running any other step.
1. Start a transaction
2. Get the instant time P of the desired table service plan to execute.
3. If P has an active heartbeat, fail and abort the transaction
4. Start a heartbeat for P
5. End the transaction

Once the rest of the clustering/compaction API logic completes or fails, the heartbeat will be cleaned up.
The below visualization captures this modification to existing execution of immutable table service plans (the faded-out steps are ones that already exist in the current flow of immutable table service execution, as shown earlier)
![heartbeat table service lifecycle (1)](https://github.com/user-attachments/assets/a8d63614-9691-4c87-b871-fc6855095227)

A transaction is needed to ensure that if multiple concurrent callers attempt to start a heartbeat at the same time, at most one will start a heartbeat. 
Consider the interleaving of three writers attempting to execute the sample compaction plan "t" visualized below.

![flow table serivce](https://github.com/user-attachments/assets/da8ce9ec-6b03-424a-909a-1481546e520d)

Writers A and B both start to execute "t" at the same time, but due to a writer needing to check and start the heartbeat within a transaction, only one writer is able to start a heartbeat and proceed. Here, since writer B started the heartbeat first, writer A is able to see that another writer is already executing "t" and knows that it should self-abort. When writer B fails, the newer Writer C will still be able to rollback and retry the compaction plan, due to the heartbeat started by writer B now being expired. See "Test Plan" for a list of more scenarios.

Note that although this increases the contention of multiple jobs trying to acquire a table lock, because only a few DFS calls will be made during this transaction (to find the heartbeat file and view its last update) other jobs waiting to start a transaction should ideally not have to wait for too long.


### Improving reliability of mutable table service plans execution
(Need to revise based on cancellable plan proposal)

## Test Plan

A proposed fix should handle any combination of table service execution/rollback operations. A few scenarios for compaction, logcomopaction, and clustering that can be tested are listed below

### Immutable table service plan
Assume that table service plan P is immutable

| #|  Scenario | Expectations |
| - | - | -| 
|A.| Multiple writers executing  on the same new plan P , which is in a requested state | At most one attempt will complete execution and create the completed data files and commit metadata for P, the rest will either fail or return the completed metadata (due to starting after P was already completed) | 
|B.| <pre> <p> 1. Writer X starts execution of P and transitions to inflight </p> <p> 2. Writer Y starts executing P, after it has been transitioned to inflight </p> </pre> | X or Y may complete execution (or neither), though both may still succeed. If Y completed P, it would have first rolled back the existing attempt. If X completed P, Y would either fail or return completed metadata |
|C.|<pre> <p> 1. Plan P exists on the timeline, which has already been transitioned to inflight </p> <p> 2. Writer X starts executing P </p> <p> 3. Writer Y starts executing P </p> </pre> | X or Y may complete execution (or neither),  though both may still succeed. If X completed execution, it would have had to rollback P.inflight first before re-execution. If Y completed execution, it may or may not have had to do a rollback, depending on if X failed after rolling back P.inflight but before re-attempting the actual execution. |


### Mutable table service plan
Assume that table service plan P is mutable

| #|  Scenario | Expectations |
| - | - | -| 
|A.| Multiple writers executing  on the same new plan P , which is in a requested state | At most one attempt will complete execution and create the completed data files and commit metadata for P, the rest will either fail or return the completed metadata (due to starting after P was already completed) | 
|B.| <pre> <p> 1. Writer X starts execution of P and transitions to inflight </p> <p> 2. Writer Y starts executing P, after it has been transitioned to inflight </p> </pre> | X might complete execution or fail. If X ended up completing execution, Y may either fail or return completed metadata. Otherwise if X fails, Y will fail. |
|C.| <pre> <p> 1. Plan P exists on the timeline, which has already been transitioned to inflight </p> <p> 2. Writer X starts executing P </p> <p> 3. Writer Y starts executing P </p> | Both X and Y will fail |
|D.|<pre> <p> 1. Plan P is scheduled on timeline </p> <p> 2. Writer X starts executing P <p> 3. Writer Y executes Clean </p> </pre> | X may complete execution of P or fail. If X is currently executing P or P was newer than `table_service_rollback_delay` and not transitioned to inflight yet, the Y should not attempt to rollback P. Otherwise Y may try rollback P. |
|E.| <pre> <p> 1. Plan P is scheduled on timeline </p> <p> 2. Writer X executes clean </p> <p> 3. Writer Y starts executing P </p> </pre> | If P is newer than `table_service_rollback_delay` then X should ignore P, and Y will have the chance to try to execute it. Otherwise, X will try to rollback P. The only scenario where Y might actually start executing P and transitions it to inflight/completed is if X happened to fail before creating a rollback plan. Otherwise, Y is expected to fail. |

## Rollout/Adoption Plan

- In order to not impact existing users, `table_service_rollback_delay` can be set to 0, which will allow the current clean behavior (where removeable plans may be prematurely rolled-back) to remain.
- There is a small possibility that users using concurrent writers to execute table services may see that, after an execution attempt of a table service plan failed, retries within the next few minutes may all fail as well. This is since there is a chance that a writer may fail and terminate before it has a chance to cleanup it's heartbeat. In order to address this potential (but unlikely) scenario, the new changes to compact/cluster execution can be made to only trigger if a new config flag is enabled. 

