# WikiPulse edit-war candidate evaluation

Evaluation date: 2026-10-24. This is role B's correctness evidence for the heuristic; it is not a claim that every flagged page contains a genuine content dispute.

## Input and sampling

- Fixed input: A's compact handoff `wikipulse_bc_handoff_20261013_0800_compact`, Delta version 0.
- Observed event time ends at 2026-10-13 05:59:59 UTC. Following the B/C contract, only windows ending no later than 05:57:59 are eligible.
- The production rule found 24 overlapping windows. They were collapsed by `(wiki, title)` and the strongest window per page was inspected, giving all 4 unique candidate pages; this is a census of the snapshot, not a random sample.
- A **true positive** requires evidence of a substantive disagreement over article content, sourcing or interpretation. Routine vandalism rollback, sockpuppet cleanup and patrol-tool reversions are negative even when users alternate A → other → A.

The evidence was generated with `scripts/evaluate_wikipulse_edit_wars.py`. Labels below were assigned manually from the ordered users and edit summaries.

## Labels

| Wiki / page | Strongest window evidence | Label | Reason |
| --- | --- | --- | --- |
| enwiki / `Gen Z stare` | 3 reverts, 2 users: `Dr vulpes → ~2026-54869-64 → Dr vulpes` | Negative | The registered editor's summaries explicitly identify “non-constructive edits”; this is an anti-vandalism exchange, not a demonstrated content dispute. |
| enwiki / `Lincoln Loud` | 12 reverts, 5 users, 7 adjacent user switches | Negative | Summaries include “Sockpuppet”, repeated rollback of temporary accounts and “Nuh uh”; the evidence is coordinated cleanup of disruptive edits. |
| enwiki / `Marché des Enfants-Rouges` | 3 reverts, 2 users: `FajreroCintilo → ~2026-54866-34 → FajreroCintilo` | **Positive** | The final summary states that source [5] does not support the attached claim and that other sources do not appear to support it. This is a substantive sourcing/content disagreement. |
| enwiki / `West Virginia` | 5 reverts, 3 users | Negative | One temporary account rapidly undoes several old revisions and two patrollers restore the article; the summaries show disruption cleanup, not two sides arguing a content position. |

Page-level precision for substantive edit disputes is therefore **1 / 4 = 25%** on this snapshot.

## Interpretation and handoff

The heuristic successfully identifies intense revert interaction, but it is a candidate generator rather than an edit-war classifier. The dominant false-positive mode is exactly the one anticipated in the contract: human or unflagged anti-vandal accounts alternate with a disruptive editor and satisfy the same sequence rule as a genuine dispute. C's dashboard must call these rows **potential edit-war candidates**, show the ordered reverter sequence/window, and avoid presenting the 25% estimate as a general population rate.

The estimate is limited to 8.2 hours, four English-Wikipedia pages and edit-summary evidence; it does not establish precision across languages or days. A larger multi-day replay should use the same page-level de-overlap and label rubric, and should report uncertain cases separately rather than forcing them positive.
